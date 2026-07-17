//! Spatial (bounding-box) indexes as linear quadtrees on the existing
//! index btree.
//!
//! A spatial index definition names four dotted paths holding a document's
//! west/south/east/north bbox edges (a point doc simply names the same
//! lon/lat paths twice). Each document is stored under one key: the quadkey
//! of the smallest world-grid quad cell that fully contains its bbox — a
//! string over the digits '0'..'3', one digit per subdivision level, so a
//! cell's whole subtree is a contiguous key range of the btree.
//!
//! Queries walk the quadtree top-down against a query rectangle and emit
//! btree ranges: a subtree scan for cells fully inside the rectangle, an
//! exact-cell scan (entries pinned at that level straddle a midline) plus
//! recursion for cells partly inside. Every candidate is then post-filtered
//! by the full Mango selector — like every other index here — so the index
//! may return false positives (never false negatives) and stays exact.
//!
//! Documents whose four values are not a sane finite rect (non-numbers,
//! NaN, min > max, outside lon [-180,180] / lat [-90,90]) go to a junk
//! bucket ("4", prefix-free w.r.t. real quadkeys) that every query scans,
//! preserving CouchDB's collation-based range semantics for odd data.

/// Maximum subdivision depth: 2^24 cells per axis ≈ 2 m of longitude at
/// the equator — far below any real field or viewport.
pub const MAX_LEVEL: usize = 24;

/// The bucket for documents whose bbox cannot be placed in the quadtree.
pub const JUNK: &str = "4";

/// How many levels past the query rectangle's own fit level the walk keeps
/// splitting boundary cells before settling for subtree scans (bounded
/// false positives instead of unbounded range counts).
const WALK_SLACK: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Rect {
    pub w: f64,
    pub s: f64,
    pub e: f64,
    pub n: f64,
}

impl Rect {
    pub const WORLD: Rect = Rect {
        w: -180.0,
        s: -90.0,
        e: 180.0,
        n: 90.0,
    };

    fn valid(&self) -> bool {
        self.w.is_finite()
            && self.s.is_finite()
            && self.e.is_finite()
            && self.n.is_finite()
            && self.w <= self.e
            && self.s <= self.n
    }

    fn in_world(&self) -> bool {
        self.w >= Rect::WORLD.w
            && self.e <= Rect::WORLD.e
            && self.s >= Rect::WORLD.s
            && self.n <= Rect::WORLD.n
    }

    /// Touching edges count as intersecting — bbox clauses are >=/<=.
    fn intersects(&self, o: &Rect) -> bool {
        !(self.e < o.w || self.w > o.e || self.n < o.s || self.s > o.n)
    }

    fn contains(&self, o: &Rect) -> bool {
        o.w >= self.w && o.e <= self.e && o.s >= self.s && o.n <= self.n
    }

    fn child(&self, digit: u8) -> Rect {
        let mid_lon = (self.w + self.e) / 2.0;
        let mid_lat = (self.s + self.n) / 2.0;
        Rect {
            w: if digit & 1 == 0 { self.w } else { mid_lon },
            e: if digit & 1 == 0 { mid_lon } else { self.e },
            s: if digit & 2 == 0 { self.s } else { mid_lat },
            n: if digit & 2 == 0 { mid_lat } else { self.n },
        }
    }
}

/// The quadkey a document bbox is stored under: descend while the bbox fits
/// entirely in one child. Unplaceable rects land in the junk bucket.
pub fn quadkey(rect: &Rect) -> String {
    if !rect.valid() || !rect.in_world() {
        return JUNK.to_string();
    }
    let mut key = String::new();
    let mut cell = Rect::WORLD;
    'descend: while key.len() < MAX_LEVEL {
        let mid_lon = (cell.w + cell.e) / 2.0;
        let mid_lat = (cell.s + cell.n) / 2.0;
        // A rect touching a midline fits in neither half: the low child is
        // [lo, mid), the high child [mid, hi] (upper world edges included).
        let dx = if rect.e < mid_lon {
            0
        } else if rect.w >= mid_lon {
            1
        } else {
            break 'descend;
        };
        let dy = if rect.n < mid_lat {
            0
        } else if rect.s >= mid_lat {
            2
        } else {
            break 'descend;
        };
        let digit = dx + dy;
        key.push(char::from(b'0' + digit));
        cell = cell.child(digit);
    }
    key
}

/// One btree range of a covering: the cell's quadkey and whether to scan
/// its whole subtree (`true`) or only entries pinned exactly at the cell.
#[derive(Clone, Debug, PartialEq)]
pub struct Cover {
    pub key: String,
    pub subtree: bool,
}

/// Decompose a query rectangle into disjoint btree ranges that together
/// contain every stored bbox intersecting it (plus the junk bucket).
/// Half-open queries are fine: pass ±INFINITY edges.
pub fn covering(q: &Rect) -> Vec<Cover> {
    let mut out = vec![Cover {
        key: JUNK.to_string(),
        subtree: false,
    }];
    // clamp to world so containment tests behave with infinite edges
    let q = Rect {
        w: q.w.max(Rect::WORLD.w),
        s: q.s.max(Rect::WORLD.s),
        e: q.e.min(Rect::WORLD.e),
        n: q.n.min(Rect::WORLD.n),
    };
    if q.w > q.e || q.s > q.n {
        return out; // empty rectangle still scans junk (collation edge cases)
    }
    let stop = (fit_level(&q) + WALK_SLACK).min(MAX_LEVEL);
    walk(String::new(), Rect::WORLD, &q, stop, &mut out);
    out
}

/// The depth of the smallest single cell that could contain the rectangle —
/// the walk stops splitting boundary cells a few levels below this.
fn fit_level(q: &Rect) -> usize {
    quadkey(q).len().min(MAX_LEVEL)
}

fn walk(key: String, cell: Rect, q: &Rect, stop: usize, out: &mut Vec<Cover>) {
    if !cell.intersects(q) {
        return;
    }
    if q.contains(&cell) || key.len() >= stop {
        out.push(Cover { key, subtree: true });
        return;
    }
    // Entries pinned at this cell straddle a midline; they are candidates.
    out.push(Cover {
        key: key.clone(),
        subtree: false,
    });
    for digit in 0..4u8 {
        let mut child_key = key.clone();
        child_key.push(char::from(b'0' + digit));
        walk(child_key, cell.child(digit), q, stop, out);
    }
}

/// End key for a subtree scan: the first string after every key with this
/// prefix. Quadkeys only use '0'..'3', so appending '4' bounds the prefix.
pub fn subtree_end(key: &str) -> String {
    format!("{key}4")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(w: f64, s: f64, e: f64, n: f64) -> Rect {
        Rect { w, s, e, n }
    }

    #[test]
    fn quadkey_placement() {
        // world-straddling rect stays at the root
        assert_eq!(quadkey(&r(-1.0, -1.0, 1.0, 1.0)), "");
        // NE point descends to max level
        assert_eq!(quadkey(&r(10.0, 10.0, 10.0, 10.0)).len(), MAX_LEVEL);
        assert!(quadkey(&r(10.0, 10.0, 10.0, 10.0)).starts_with('3'));
        // SW quadrant
        assert!(quadkey(&r(-10.0, -10.0, -9.0, -9.0)).starts_with('0'));
        // junk: NaN, inverted, out of world
        assert_eq!(quadkey(&r(f64::NAN, 0.0, 1.0, 1.0)), JUNK);
        assert_eq!(quadkey(&r(2.0, 0.0, 1.0, 1.0)), JUNK);
        assert_eq!(quadkey(&r(-200.0, 0.0, 1.0, 1.0)), JUNK);
        // world edges are inside
        assert_eq!(quadkey(&r(179.9, 89.9, 180.0, 90.0)).is_empty(), false);
    }

    #[test]
    fn covering_ranges_are_disjoint() {
        let covers = covering(&r(9.0, 48.0, 10.0, 49.0));
        for (i, a) in covers.iter().enumerate() {
            for b in covers.iter().skip(i + 1) {
                if a.subtree {
                    assert!(!b.key.starts_with(&a.key) || b.key == a.key);
                }
                if b.subtree {
                    assert!(!a.key.starts_with(&b.key) || a.key == b.key);
                }
                assert_ne!(a.key, b.key, "duplicate cell {}", a.key);
            }
        }
    }

    /// The no-false-negative property: for any stored bbox intersecting the
    /// query, its quadkey is covered by some emitted range.
    #[test]
    fn covering_finds_every_intersecting_bbox() {
        let covered = |covers: &[Cover], key: &str| {
            covers.iter().any(|c| {
                if c.subtree {
                    key.starts_with(c.key.as_str())
                } else {
                    key == c.key
                }
            })
        };
        // deterministic pseudo-random rects (no external rand dep)
        let mut seed = 0x243F6A8885A308D3u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed >> 11) as f64 / (1u64 << 53) as f64
        };
        let mut rects = Vec::new();
        for _ in 0..300 {
            let w = -180.0 + 360.0 * next();
            let s = -90.0 + 180.0 * next();
            let dw = 20.0 * next() * next();
            let dh = 10.0 * next() * next();
            rects.push(r(w, s, (w + dw).min(180.0), (s + dh).min(90.0)));
        }
        for qi in 0..60 {
            let q = if qi % 7 == 0 {
                // half-open query (only two of four clauses)
                r(-180.0 + 360.0 * next(), f64::NEG_INFINITY, 180.0, 90.0)
            } else {
                let w = -180.0 + 360.0 * next();
                let s = -90.0 + 180.0 * next();
                r(w, s, (w + 30.0 * next()).min(180.0), (s + 15.0 * next()).min(90.0))
            };
            let covers = covering(&q);
            assert!(covered(&covers, JUNK), "junk bucket must always be scanned");
            for rect in &rects {
                if rect.intersects(&q) {
                    let key = quadkey(rect);
                    assert!(
                        covered(&covers, &key),
                        "bbox {rect:?} (key {key}) intersects {q:?} but is not covered"
                    );
                }
            }
        }
    }

    #[test]
    fn covering_stays_small() {
        // a ~100 m vehicle-radius query must not explode into thousands of ranges
        let covers = covering(&r(9.180, 48.770, 9.182, 48.772));
        assert!(covers.len() < 600, "covering too large: {}", covers.len());
    }
}

use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod dams {
    use nadi_core::attrs::{Attribute, FromAttribute};
    use nadi_core::nadi_plugin::node_func;
    use nadi_core::node::NodeInner;

    /// Count the number of dams upstream at each point
    #[node_func(outvar = "DAMS_COUNT")]
    fn count_dams(node: &mut NodeInner, outvar: String) {
        let mut count: i64 = 0;
        for i in node.inputs() {
            let n = i.lock();
            let nc = n
                .attr(&outvar)
                .and_then(i64::from_attr)
                .unwrap_or(0);
            count += nc;
            if !n.name().starts_with("USGS") {
                count += 1;
            }
        }
        node.set_attr(&outvar, Attribute::Integer(count));
    }

    /// Count the number of gages upstream at each point
    #[node_func(outvar = "GAGES_COUNT")]
    fn count_gages(node: &mut NodeInner, outvar: String) {
        let mut count: i64 = 0;
        for i in node.inputs() {
            let n = i.lock();
            let nc = n
                .attr(&outvar)
                .and_then(i64::from_attr)
                .unwrap_or(0);
            count += nc;
            if n.name().starts_with("USGS") {
                count += 1;
            }
        }
        node.set_attr(&outvar, Attribute::Integer(count));
    }

    /// Propagage the minimum year downstream
    #[node_func(write_var = "MIN_YEAR")]
    fn min_year(node: &mut NodeInner, yearattr: String, write_var: String) {
        let mut min_yr = node.attr(&yearattr).and_then(i64::from_attr);
        for i in node.inputs() {
            let n = i.lock();
            if let Some(yr) = n.attr(&write_var).and_then(i64::from_attr) {
                min_yr = match min_yr {
                    Some(m) => {
                        if yr < m {
                            Some(yr)
                        } else {
                            Some(m)
                        }
                    }
                    None => Some(yr),
                }
            }
        }
        if let Some(yr) = min_yr {
            node.set_attr(&write_var, Attribute::Integer(yr));
        }
    }
}

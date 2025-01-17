use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod dams {
    use nadi_core::attrs::{Attribute, FromAttribute, HasAttributes};
    use nadi_core::nadi_plugin::node_func;
    use nadi_core::node::NodeInner;

    /// Count the number of nodes upstream at each point that satisfies a certain condition
    #[node_func]
    fn count_node_if(node: &mut NodeInner, count_attr: &str, cond: bool) -> Attribute {
        let mut count = cond as i64;
        for i in node.inputs() {
            let n = i.lock();
            let nc = n.attr(count_attr).and_then(i64::from_attr).unwrap_or(0);
            count += nc;
        }
        Attribute::Integer(count)
    }

    /// Propagate the minimum year downstream
    #[node_func(write_var = "MIN_YEAR")]
    fn min_year(node: &mut NodeInner, yearattr: &str, write_var: &str) {
        let mut min_yr = node.attr(yearattr).and_then(i64::from_attr);
        for i in node.inputs() {
            let n = i.lock();
            if let Some(yr) = n.attr(write_var).and_then(i64::from_attr) {
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
            node.set_attr(write_var, Attribute::Integer(yr));
        }
    }
}

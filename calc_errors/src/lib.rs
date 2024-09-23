use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod errors {
    use nadi_core::nadi_plugin::{network_func, node_func};
    use nadi_core::{
        attrs::{Attribute, FromAttributeRelaxed},
        Network, NodeInner,
    };

    /** Calculate Error from two timeseries values in the node

    It calculates the error using two attribute values from all the nodes.

    Arguments:
    - ts1: String      Timeseries value to use as actual value
    - ts2: String      Timeseries value to be used to calculate the error
    - error: String    Error type: rmse/nrmse/abserr/nse [default: rmse]
    - outattr: String  Attribute to save the output on [default: ERROR]
    - print: bool      Print the output to stdout [default: false]
    */
    #[node_func(error = "rmse", outattr = "ERROR", print = false)]
    fn calc_ts_error(
        node: &mut NodeInner,
        ts1: String,
        ts2: String,
        error: String,
        outattr: String,
        print: bool,
    ) -> Result<f64, String> {
        let obs: &[f64] = node.try_ts(&ts1)?.try_values()?;
        let sim: &[f64] = node.try_ts(&ts2)?.try_values()?;
        let err = calc_error(&obs, &sim, &error)?;
        if print {
            println!("{}:{}={}", node.name(), error, err);
        }
        node.set_attr(&outattr, Attribute::Float(err));
        Ok(err)
    }

    /** Calculate Error from two attribute values in the network

    It calculates the error using two attribute values from all the nodes.

    Arguments:
    - attr1: String    Attribute value to use as actual value
    - attr2: String    Attribute value to be used to calculate the error
    - error: String    Error type: rmse/nrmse/abserr/nse [default: rmse]
    */
    #[network_func(error = "rmse")]
    fn calc_attr_error(
        net: &mut Network,
        attr1: String,
        attr2: String,
        error: String,
    ) -> Result<f64, String> {
        let obs: Vec<f64> = attr_as_vec(net, &attr1);
        let sim: Vec<f64> = attr_as_vec(net, &attr2);
        let err = calc_error(&obs, &sim, &error)?;
        println!("{}={}", error, err);
        Ok(err)
    }

    fn attr_as_vec(net: &Network, attr: &str) -> Vec<f64> {
        net.nodes()
            .map(|n| {
                n.lock()
                    .attr(attr)
                    .map(|a| f64::from_attr_relaxed(a))
                    .flatten()
                    .unwrap_or(f64::NAN)
            })
            .collect()
    }

    fn calc_error(obs: &[f64], sim: &[f64], error: &str) -> Result<f64, String> {
        let err = match error {
            "rmse" => calc_rmse(obs, sim),
            "nrmse" => calc_nrmse(obs, sim),
            "abserr" => calc_abserr(obs, sim),
            "nse" => calc_nse(obs, sim),
            _ => return Err(String::from("Unknown Error type")),
        };
        Ok(err)
    }

    fn calc_rmse(obs: &[f64], sim: &[f64]) -> f64 {
        let mut count: usize = 0;
        let mut sum_e: f64 = 0.0;
        obs.iter().zip(sim).for_each(|(kd, cd)| {
            if !kd.is_nan() && !cd.is_nan() {
                sum_e += (cd - kd).powi(2);
                count += 1;
            }
        });
        // not normalized
        (sum_e / count as f64).sqrt()
    }

    fn calc_nrmse(obs: &[f64], sim: &[f64]) -> f64 {
        let mut total: f64 = 0.0;
        let mut count: usize = 0;
        let mut sum_e: f64 = 0.0;
        obs.iter().zip(sim).for_each(|(kd, cd)| {
            if !kd.is_nan() && !cd.is_nan() {
                sum_e += (cd - kd).powi(2);
                total += kd;
                count += 1;
            }
        });
        // normalized
        (sum_e / count as f64).sqrt() / (total / count as f64)
    }

    fn calc_abserr(obs: &[f64], sim: &[f64]) -> f64 {
        let d = obs.iter().zip(sim).filter_map(|(kd, cd)| {
            if kd.is_nan() || cd.is_nan() {
                None
            } else {
                Some((cd - kd).abs())
            }
        });
        d.clone().sum::<f64>() / (d.count() as f64)
    }

    fn calc_nse(obs: &[f64], sim: &[f64]) -> f64 {
        let non_nan = obs.iter().filter(|q| !q.is_nan());
        let mean = non_nan.clone().sum::<f64>() / (non_nan.count() as f64);
        let mut mse: f64 = 0.0;
        let mut denom: f64 = 0.0;
        obs.iter().zip(sim).for_each(|(kd, cd)| {
            if !kd.is_nan() && !cd.is_nan() {
                mse += (cd - kd) * (cd - kd);
                denom += (mean - kd) * (mean - kd)
            }
        });
        1.0 - mse / denom
    }
}

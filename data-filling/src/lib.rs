use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod datafill {
    use super::utils::{DataFillMethod, ExprFunc};
    use nadi_core::abi_stable::external_types::RMutex;
    use nadi_core::abi_stable::std_types::RArc;
    use nadi_core::anyhow::{self, bail, Context};
    use nadi_core::nadi_plugin::{network_func, node_func};
    use nadi_core::prelude::*;
    use nadi_core::string_template::Template;
    use nadi_core::timeseries;
    use polars::prelude::*;
    use rand::{rngs::StdRng, SeedableRng};
    use std::collections::HashMap;
    use std::fs::File;
    use std::io::{BufWriter, Write};
    use std::ops::Mul;
    use std::path::PathBuf;

    #[node_func(method = DataFillMethod::Linear, dtype = "Floats")]
    fn load_csv_fill(
        node: &mut NodeInner,
        /// Name of the timeseries
        name: String,
        /// Template of the CSV file for the nodes
        file: Template,
        /// date time format, if you only have date, but have time on format string, it will panic
        timefmt: String,
        /// Names of date column and value column
        columns: (String, String),
        // columns: Option<(String, String)>,
        /// Method to use for data filling: forward/backward/linear
        method: DataFillMethod,
        /// DataType to load into timeseries
        dtype: String,
    ) -> anyhow::Result<()> {
        let (dtcol, valcol) = columns;
        let file = node.render(&file)?;
        let df = LazyCsvReader::new(file)
            .with_has_header(true)
            .with_try_parse_dates(true)
            .finish()?;
        // converting the dates to timeline that all timeseries can share
        let dates = df
            .clone()
            .lazy()
            .select([col(&dtcol)
                .dt()
                .timestamp(TimeUnit::Milliseconds)
                .alias("timestamp")])
            .collect()?;

        let dt_col = dates.column("timestamp")?;
        let start = dt_col.min::<i64>()?.context("No minimum date")?;
        let end = dt_col.max::<i64>()?.context("No maximum date")?;
        let step = (end - start) / dt_col.len() as i64;

        let dates = df
            .clone()
            .lazy()
            .select([col(&dtcol).dt().strftime(&timefmt)])
            .collect()?;
        let dt_col = dates.column(&dtcol)?;
        let dates: Vec<&str> = dt_col.str()?.into_no_null_iter().collect();
        let timeline = nadi_core::timeseries::TimeLineInner::new(
            start,
            end,
            step,
            true,
            dates.into_iter().map(String::from).collect(),
            &timefmt,
        );
        let fill: ExprFunc = method.polars_fn()?;
        let values: timeseries::Series = match dtype.as_str() {
            "Floats" => {
                let df2 = df
                    .clone()
                    .lazy()
                    .select([fill(col(&valcol).cast(DataType::Float64))])
                    .collect()?;
                let s = df2.column(&valcol)?;
                let v: Vec<f64> = s.f64()?.into_no_null_iter().collect();
                timeseries::Series::floats(v)
            }
            _ => bail!("{dtype} is not supported or is not a recognized data type"),
        };
        let timeline = RArc::new(RMutex::new(timeline));
        let ts = nadi_core::timeseries::TimeSeries::new(timeline.clone(), values);
        node.set_ts(&name, ts);
        Ok(())
    }

    /// Write the given nodes to csv with given attributes and experiment results
    #[network_func]
    fn save_experiments_csv(
        net: &mut Network,
        #[prop] prop: &Propagation,
        /// Path to the output csv
        outfile: PathBuf,
        /// list of attributes to write
        attrs: Vec<String>,
        /// Prefix
        prefix: String,
        /// list of errors to write
        errors: Vec<String>,
    ) -> anyhow::Result<()> {
        let f = File::create(&outfile)?;
        let mut w = BufWriter::new(f);
        let middle = !attrs.is_empty() && !errors.is_empty();
        // headers for the csv
        writeln!(
            w,
            "{}{}experiment,method,{}",
            attrs.join(","),
            if middle { "," } else { "" },
            errors.join(",")
        )?;
        let methods = ["forward", "backward", "linear", "iratio", "oratio"];
        for node in net.nodes_propagation(prop).map_err(anyhow::Error::msg)? {
            let node = node.lock();
            let attrs: Vec<String> = attrs
                .iter()
                .map(|a| node.attr(a).map(|a| a.to_string()).unwrap_or_default())
                .collect();

            for m in methods {
                let series: Vec<Vec<String>> = errors
                    .iter()
                    .map(|e| {
                        node.series(&format!("{prefix}_{m}_{e}"))
                            .map(|s| {
                                s.clone()
                                    .to_attributes()
                                    .into_iter()
                                    .map(|a| a.to_string())
                                    .collect()
                            })
                            .unwrap_or_default()
                    })
                    .collect();
                let lengths: Vec<usize> = series.iter().map(|s| s.len()).collect();
                if errors.is_empty() {
                    writeln!(w, "{}", attrs.join(","))?;
                    continue;
                } else if lengths.iter().any(|l| *l != lengths[0]) {
                    return Err(anyhow::Error::msg(format!(
                        "Node {}: Series lengths don't match: {lengths:?}",
                        node.name()
                    )));
                }
                for i in 0..lengths[0] {
                    let values: Vec<&str> = series.iter().map(|s| s[i].as_str()).collect();
                    writeln!(
                        w,
                        "{}{}{i},{m},{}",
                        attrs.join(","),
                        if middle { "," } else { "" },
                        values.join(",")
                    )?;
                }
            }
        }
        Ok(())
    }

    #[node_func(experiments = 10usize, samples = 100usize)]
    fn datafill_experiment(
        node: &mut NodeInner,
        /// Prefix for name of the series to save metrics on
        name: String,
        /// Template of the CSV file for the nodes
        file: Template,
        /// Variable to use for inputratio/outputratio methods
        ratio_var: String,
        // todo: make a String or Int datatype and impl FromAttribute
        /// Names of date column and value column
        columns: Option<(String, String)>,
        /// Number of experiements to run
        experiments: usize,
        /// Number of samples on each experiment
        samples: usize,
    ) -> anyhow::Result<()> {
        let csv = node.render(&file)?;
        let (dtcol, valcol) = match &columns {
            Some((dt, val)) => (col(dt), col(val)),
            None => (nth(0), nth(1)),
        };
        let dtname = columns.as_ref().map(|(d, _)| d.as_str()).unwrap_or("date");
        let mut df = LazyCsvReader::new(csv)
            .with_has_header(columns.is_some())
            .with_try_parse_dates(true)
            .with_infer_schema_length(Some(10))
            .with_ignore_errors(true)
            .with_schema_modify(|mut s| {
                // sometimes it infers it as integer based on first 10 values
                s.set_dtype_at_index(1, DataType::Float64);
                Ok(s)
            })?
            .finish()?
            .with_columns([
                dtcol.clone().alias(dtname),
                valcol.clone().alias(node.name()),
            ])
            .select([col(dtname), col(node.name())]);
        node.inputs()
            .iter()
            .chain(node.output().into_option())
            .try_for_each(|n| -> anyhow::Result<()> {
                let n = n.lock();
                let csv = n.render(&file)?;
                let idf = LazyCsvReader::new(csv)
                    .with_has_header(columns.is_some())
                    .with_try_parse_dates(true)
                    .with_infer_schema_length(Some(10))
                    .with_ignore_errors(true)
                    .with_schema_modify(|mut s| {
                        // sometimes it infers it as integer based on first 10 values
                        s.set_dtype_at_index(1, DataType::Float64);
                        Ok(s)
                    })?
                    .finish()?
                    .with_columns([dtcol.clone().alias(dtname), valcol.clone().alias(n.name())])
                    .select([col(dtname), col(n.name())]);
                df = df.clone().join(
                    idf,
                    [col(dtname)],
                    [col(dtname)],
                    JoinArgs::new(JoinType::Left),
                );
                Ok(())
            })?;
        let df2 = df.drop_nulls(None).collect()?;
        let ht = df2.height();
        if ht < (samples / 10) {
            println!(
                "Warn: Node {} doesn't have enough values ({ht}) to experiment, skipping",
                node.name()
            );
            return Ok(());
        }
        let err_metrics = ["rmse", "nrmse", "abserr", "nse"];
        let mut errors: HashMap<(&'static str, &'static str), Vec<f64>> = HashMap::new();
        // ["rmse", "nrmse", "abserr", "nse"]
        //     .into_iter()
        //     .map(|k| (k, Vec::with_capacity(experiments)))
        //     .collect();
        let fill_methods = [
            (
                "forward",
                DataFillMethod::Strategy(FillNullStrategy::Forward(None)),
            ),
            (
                "backward",
                DataFillMethod::Strategy(FillNullStrategy::Backward(None)),
            ),
            ("linear", DataFillMethod::Linear),
        ];
        for i in 0..experiments {
            let mut rng = StdRng::from_rng(&mut rand::rng());
            let indices: Vec<i64> = rand::seq::index::sample(&mut rng, ht, samples)
                .iter()
                .map(|i| i as i64)
                .collect();
            let vals = Series::new("nulls".into(), indices);

            let df = df2
                .clone()
                .lazy()
                .with_column(
                    col(dtname)
                        .cum_count(false)
                        .is_in(lit(vals))
                        .alias("sample"),
                )
                .with_column(
                    when(col("sample"))
                        .then(lit(NULL))
                        .otherwise(col(node.name()))
                        .alias("new_vals"),
                )
                .collect()?;
            for (mname, method) in &fill_methods {
                let fill: ExprFunc = method.polars_fn()?;
                let mut df = df
                    .clone()
                    .lazy()
                    .with_column(fill(col("new_vals")))
                    .filter(col("sample"))
                    .collect()?;
                let mut file =
                    std::fs::File::create(format!("/tmp/experiments/{i}-{mname}.csv")).unwrap();
                CsvWriter::new(&mut file).finish(&mut df).unwrap();
                let obs: Vec<f64> = df.column(node.name())?.f64()?.into_no_null_iter().collect();
                let sim: Vec<f64> = df.column("new_vals")?.f64()?.into_no_null_iter().collect();
                for e in &err_metrics {
                    let errs = errors
                        .entry((mname, e))
                        .or_insert_with(|| Vec::with_capacity(experiments));
                    errs.push(calc_error(&obs, &sim, e).expect("should be a known error"));
                }
            }
            // input ratio
            let var = &ratio_var;
            let val: f64 = node.try_attr(var).unwrap_or(0.0);
            let oval: f64 = node
                .output()
                .map(|o| o.lock().try_attr(var).unwrap_or(0.0))
                .unwrap_or(0.0);
            let ival: f64 = node
                .inputs()
                .iter()
                .map(|n| n.lock().try_attr::<f64>(var).unwrap_or(0.0))
                .sum();
            let iratio = val / ival;
            let oratio = val / oval;
            let isum: Vec<Expr> = node.inputs().iter().map(|n| col(n.lock().name())).collect();
            let ifill = |mut e: Expr| -> Expr {
                for i in isum {
                    e = e + i;
                }
                e.mul(lit(iratio))
            };
            let oname = node
                .output()
                .map(|o| o.lock().name().to_string())
                .unwrap_or_default();
            let mut df = df
                .clone()
                .lazy()
                .with_column(ifill(lit(0)).alias("iratio_vals"))
                .with_column(col(&oname).mul(lit(oratio)).alias("oratio_vals"))
                .with_column(
                    when(col("new_vals").is_null())
                        .then(col("iratio_vals"))
                        .otherwise(col("new_vals"))
                        .alias("iratio_fills"),
                )
                .with_column(
                    when(col("new_vals").is_null())
                        .then(col("oratio_vals"))
                        .otherwise(col("new_vals"))
                        .alias("oratio_fills"),
                )
                .filter(col("sample"))
                .collect()?;
            let mut file =
                std::fs::File::create(format!("/tmp/experiments/{i}-ratio.csv")).unwrap();
            CsvWriter::new(&mut file).finish(&mut df).unwrap();
            let obs: Vec<f64> = df.column(node.name())?.f64()?.into_no_null_iter().collect();
            let sim1: Vec<f64> = df
                .column("iratio_fills")?
                .f64()?
                .into_no_null_iter()
                .collect();
            let sim2: Vec<f64> = df
                .column("oratio_fills")?
                .f64()?
                .into_no_null_iter()
                .collect();
            for e in err_metrics {
                let errs = errors
                    .entry(("iratio", e))
                    .or_insert_with(|| Vec::with_capacity(experiments));
                errs.push(calc_error(&obs, &sim1, e).expect("should be a known error"));
                let errs = errors
                    .entry(("oratio", e))
                    .or_insert_with(|| Vec::with_capacity(experiments));
                errs.push(calc_error(&obs, &sim2, e).expect("should be a known error"));
            }
        }
        for ((mname, e), errs) in errors {
            node.set_series(&format!("{name}_{mname}_{e}"), errs.into());
        }
        Ok(())
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

mod utils {
    use nadi_core::anyhow;
    use nadi_core::prelude::*;
    use polars::prelude::*;

    pub type ExprFunc = fn(Expr) -> Expr;

    #[derive(Debug)]
    pub enum DataFillMethod {
        Strategy(FillNullStrategy),
        Linear,
        Nearest,
        InputRatio(String),
        OutputRatio(String),
    }

    impl DataFillMethod {
        pub fn polars_fn(&self) -> anyhow::Result<ExprFunc> {
            Ok(match self {
                // All this because we cannot capture s in a fn
                DataFillMethod::Strategy(s) => match s {
                    FillNullStrategy::Forward(None) => |e: Expr| -> Expr { e.forward_fill(None) },
                    FillNullStrategy::Backward(None) => |e: Expr| -> Expr { e.backward_fill(None) },
                    FillNullStrategy::Mean => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Mean) }
                    }
                    FillNullStrategy::Min => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Min) }
                    }
                    FillNullStrategy::Max => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Max) }
                    }
                    FillNullStrategy::Zero => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::Zero) }
                    }
                    FillNullStrategy::One => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::One) }
                    }
                    FillNullStrategy::MaxBound => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::MaxBound) }
                    }
                    FillNullStrategy::MinBound => {
                        |e: Expr| -> Expr { e.fill_null_with_strategy(FillNullStrategy::MinBound) }
                    }
                    _ => return Err(anyhow::Error::msg("Not implemented")),
                },
                DataFillMethod::Linear => {
                    |e: Expr| -> Expr { e.interpolate(InterpolationMethod::Linear) }
                }
                DataFillMethod::Nearest => {
                    |e: Expr| -> Expr { e.interpolate(InterpolationMethod::Nearest) }
                }
                _ => return Err(anyhow::Error::msg("Not implemented")),
            })
        }
    }

    impl FromAttribute for DataFillMethod {
        fn from_attr(value: &Attribute) -> Option<Self> {
            FromAttribute::try_from_attr(value).ok()
        }
        fn try_from_attr(value: &Attribute) -> Result<Self, String> {
            let strval = String::try_from_attr(value)?;
            let (name, data) = strval.split_once(':').unwrap_or((&strval, ""));
            let no_data = |m: DataFillMethod| -> Result<Self, String> {
                if data.is_empty() {
                    Ok(m)
                } else {
                    Err(format!("Unused part {:?} for Data fill method", data))
                }
            };
            match name {
                "backward" => no_data(Self::Strategy(FillNullStrategy::Backward(None))),
                "forward" => no_data(Self::Strategy(FillNullStrategy::Forward(None))),
                "mean" => no_data(Self::Strategy(FillNullStrategy::Mean)),
                "min" => no_data(Self::Strategy(FillNullStrategy::Min)),
                "max" => no_data(Self::Strategy(FillNullStrategy::Max)),
                "zero" => no_data(Self::Strategy(FillNullStrategy::Zero)),
                "one" => no_data(Self::Strategy(FillNullStrategy::One)),
                "maxbound" => no_data(Self::Strategy(FillNullStrategy::MaxBound)),
                "minbound" => no_data(Self::Strategy(FillNullStrategy::MinBound)),
                "linear" => no_data(Self::Linear),
                "nearest" => no_data(Self::Nearest),
                "input_ratio" | "iratio" => {
                    if data.is_empty() {
                        Err(format!("Data fill method {:?} requires variables", data))
                    } else {
                        Ok(Self::InputRatio(data.to_string()))
                    }
                }
                "output_ratio" | "oratio" => {
                    if data.is_empty() {
                        Err(format!("Data fill method {:?} requires variables", data))
                    } else {
                        Ok(Self::OutputRatio(data.to_string()))
                    }
                }
                x => Err(format!("Data fill method {x:?} not recognized")),
            }
        }
    }
}

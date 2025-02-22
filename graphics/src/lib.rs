use nadi_core::nadi_plugin::nadi_plugin;

mod colors;
mod plots;
mod timeseries;

#[nadi_plugin]
mod graphics {
    use super::colors::AttrColor;
    use super::plots::*;
    use super::timeseries;
    use abi_stable::external_types::RMutex;
    use abi_stable::std_types::{RArc, RSome, RString};
    use anyhow::{bail, Context};
    use nadi_core::nadi_plugin::{network_func, node_func};
    use nadi_core::prelude::*;
    use nadi_core::string_template::Template;
    use nadi_core::timeseries::Series;
    use polars::prelude::*;
    use std::path::PathBuf;
    use std::str::FromStr;

    /// Count the number of na values in CSV file for each nodes in a network
    ///
    /// # Arguments
    /// - `file`: Input CSV file path to read (should have column with
    ///   node names for all nodes)
    /// - `name`: Name of the timeseries
    /// - `date_col`: Date Column name
    /// - `timefmt`: date time format, if you only have date, but have time on format string, it will panic
    /// - `data_type`: Type of the data to cast into
    #[network_func(date_col = "date", timefmt = "%Y-%m-%d", data_type = "Floats")]
    fn csv_load_ts(
        net: &mut Network,
        file: PathBuf,
        name: String,
        date_col: String,
        timefmt: String,
        data_type: String,
    ) -> anyhow::Result<()> {
        let columns: Vec<&str> = net.node_names().collect();
        let df: DataFrame = LazyCsvReader::new(file)
            .with_has_header(true)
            .with_try_parse_dates(true)
            .finish()?
            .lazy()
            .select([col(&date_col), cols(&columns)])
            .collect()?;

        let values: Vec<Series> = match data_type.as_str() {
            "Floats" => {
                let df2 = df
                    .clone()
                    .lazy()
                    .select([cols(&columns).cast(DataType::Float64).fill_null(lit(0.0))])
                    .collect()?;
                let mut vals = vec![];
                for col in &columns {
                    let s = df2.column(col)?;
                    let v: Vec<f64> = s.f64()?.into_no_null_iter().collect();
                    vals.push(Series::floats(v));
                }
                vals
            }
            "Integers" => {
                let df2 = df
                    .clone()
                    .lazy()
                    .select([cols(&columns).cast(DataType::Int64).fill_null(lit(0))])
                    .collect()?;
                let mut vals = vec![];
                for col in &columns {
                    let s = df2.column(col)?;
                    let v: Vec<i64> = s.i64()?.into_no_null_iter().collect();
                    vals.push(Series::integers(v));
                }
                vals
            }
            "Strings" => {
                let df2 = df
                    .clone()
                    .lazy()
                    .select([cols(&columns)
                        .cast(DataType::String)
                        .fill_null(lit(String::new()))])
                    .collect()?;
                let mut vals = vec![];
                for col in &columns {
                    let s = df2.column(col)?;
                    let v: Vec<&str> = s.str()?.into_no_null_iter().collect();
                    vals.push(Series::strings(v.into_iter().map(RString::from).collect()));
                }
                vals
            }
            "Booleans" => {
                let df2 = df
                    .clone()
                    .lazy()
                    .select([cols(&columns).cast(DataType::Boolean).fill_null(lit(false))])
                    .collect()?;
                let mut vals = vec![];
                for col in &columns {
                    let s = df2.column(col)?;
                    let v: Vec<bool> = s.bool()?.into_no_null_iter().collect();
                    vals.push(Series::booleans(v));
                }
                vals
            }
            // "Dates" => {
            //     let df2 = df
            //         .clone()
            //         .lazy()
            //         .select([cols(&columns).cast(DataType::Date)])
            //         .collect()?;
            // }
            // "Times" => {
            //     let df2 = df
            //         .clone()
            //         .lazy()
            //         .select([cols(&columns).cast(DataType::Time)])
            //         .collect()?;
            // }
            // "DateTimes" => {
            //     let df2 = df
            //         .clone()
            //         .lazy()
            //         .select([cols(&columns).cast(DataType::Datetime(TimeUnit::Milliseconds, None))])
            //         .collect()?;
            // }
            _ => bail!("{data_type} is not supported or is not a recognized data type"),
        };

        // converting the dates to timeline that all timeseries can share
        let dates = df
            .clone()
            .lazy()
            .select([col(&date_col)
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
            .select([col(&date_col).dt().strftime(&timefmt)])
            .collect()?;
        let dt_col = dates.column(&date_col)?;
        let dates: Vec<&str> = dt_col.str()?.into_no_null_iter().collect();
        let timeline = nadi_core::timeseries::TimeLineInner::new(
            start,
            end,
            step,
            true,
            dates.into_iter().map(String::from).collect(),
            &timefmt,
        );
        let timeline = RArc::new(RMutex::new(timeline));

        for (node, vals) in net.nodes().zip(values) {
            let mut node = node.lock();
            let ts = nadi_core::timeseries::TimeSeries::new(timeline.clone(), vals);
            node.set_ts(&name, ts);
        }
        Ok(())
    }

    /// Count the number of na values in CSV file for each nodes in a network
    ///
    /// # Arguments
    /// - `file`: Input CSV file path to read (should have column with node names for all nodes)
    /// - `outattr`: Output attribute to save the count of NA to. If empty print to stdout
    /// - `sort`: show the nodes with larger gaps on top, only applicable while printing
    /// - `head`: at max show only this number of nodes
    /// - `skip_zero`: skip nodes with zero missing numbers
    #[network_func(sort = false, skip_zero = false)]
    fn csv_count_na(
        net: &mut Network,
        file: PathBuf,
        outattr: Option<String>,
        sort: bool,
        skip_zero: bool,
        head: Option<i64>,
    ) -> anyhow::Result<()> {
        let columns: Vec<&str> = net.node_names().collect();
        let dfc: DataFrame = LazyCsvReader::new(file)
            .with_has_header(true)
            .finish()?
            .lazy()
            .select([cols(&columns)])
            .with_columns([cols(&columns).is_null().cast(DataType::Int64)])
            .collect()?;

        let num_nas = columns
            .iter()
            .map(|&col| dfc.column(col).unwrap().sum::<i64>().unwrap());
        if let Some(a) = outattr {
            columns.iter().zip(num_nas).for_each(|(col, nas)| {
                net.node_by_name(col)
                    .expect("columns var was extracted from node names")
                    .lock()
                    .set_attr(&a, Attribute::Integer(nas));
            });
        } else {
            let max_len: usize = columns.iter().map(|c| c.len()).max().unwrap_or_default();
            let max_num_len = num_nas
                .clone()
                .map(|c| c.to_string().len())
                .max()
                .unwrap_or_default();
            let mut num_nas: Vec<(&&str, i64)> = columns.iter().zip(num_nas).collect();
            if skip_zero {
                num_nas = num_nas.into_iter().filter(|(_, v)| *v > 0).collect();
            }
            if sort {
                num_nas.sort_by(|a, b| b.1.cmp(&a.1));
            }
            let take = head.map(|i| i as usize).unwrap_or(columns.len());
            println!("| {2:<0$} | {3:>1$} |", max_len, max_num_len, "Node", "NAs");
            println!("|:{2:-<0$}-|-{3:->1$}:|", max_len, max_num_len, "-", "-");
            num_nas.into_iter().take(take).for_each(|(col, nas)| {
                println!("| {col:<0$} | {nas:>1$} |", max_len, max_num_len);
            });
        }
        Ok(())
    }

    /// Draw the data blocks with arrows in timeline
    #[network_func(date_col = "date", config = NetworkPlotConfig::default(), blocks_width = 500.0, fit = false)]
    fn csv_data_blocks_svg(
        net: &mut Network,
        csvfile: PathBuf,
        outfile: PathBuf,
        label: Template,
        date_col: String,
        #[relaxed] config: NetworkPlotConfig,
        blocks_width: f64,
        fit: bool,
    ) -> anyhow::Result<()> {
        timeseries::csv_data_blocks_svg(
            &net,
            csvfile,
            outfile,
            date_col,
            label,
            config,
            blocks_width,
            fit,
        )
    }

    /// Create a SVG file with the given network structure
    #[node_func(height = 80.0, width = 80.0, margin = 10.0)]
    fn attr_fraction_svg(
        node: &mut NodeInner,
        attr: &str,
        outfile: &Template,
        color: &AttrColor,
        height: f64,
        width: f64,
        margin: f64,
    ) -> anyhow::Result<()> {
        let outfile = PathBuf::from(node.render(outfile)?);
        let color = color.clone().color().context("Invalid color argument")?;
        let val: f64 = node.try_attr(attr).map_err(anyhow::Error::msg)?;
        let mut val_out = val;
        if let RSome(o) = node.output() {
            val_out = o.lock().try_attr(attr).map_err(anyhow::Error::msg)?;
        }
        let val_inps: Vec<f64> = node
            .inputs()
            .iter()
            .map(|i| i.lock().try_attr(attr))
            .collect::<Result<Vec<f64>, String>>()
            .map_err(anyhow::Error::msg)?;
        let val_inp = val_inps.iter().sum::<f64>();
        let outfrac = (val / val_out).clamp(0.0, 1.0);
        let infrac = (val_inp / val).clamp(0.0, 1.0);
        let infracs: Vec<f64> = val_inps
            .iter()
            .map(|i| (i / val_inp).clamp(0.0, 1.0))
            .collect();

        let mut surf = cairo::SvgSurface::new::<&std::path::Path>(
            width + margin * 2.0,
            height + margin * 2.0,
            Some(&outfile),
        )?;
        let ctx = cairo::Context::new(&mut surf)?;
        ctx.set_hairline(true);
        ctx.set_source_rgb(0.5, 0.5, 1.0);
        ctx.rectangle(margin, margin, width, height);
        ctx.fill()?;
        color.set(&ctx);
        // ctx.set_source_rgb(0.5, 1.0, 0.5);
        ctx.rectangle(margin, margin, width, height * outfrac);
        ctx.fill()?;
        ctx.set_source_rgb(1.0, 0.5, 0.5);
        ctx.rectangle(margin, margin, width * infrac, height * outfrac);
        ctx.fill()?;
        ctx.set_source_rgb(1.0, 0.25, 0.25);
        let mut pos = margin;
        let mut alt = true;
        for inf in infracs {
            let pos2 = height * outfrac * inf;
            if alt {
                ctx.rectangle(margin, pos, width * infrac, pos2);
                ctx.fill()?;
            }
            alt = !alt;
            pos += pos2;
        }
        Ok(())
    }

    /// Create a SVG file with the given network structure
    #[network_func(config = NetworkPlotConfig::default(), fit = false, highlight = Vec::new())]
    fn export_svg(
        net: &mut Network,
        outfile: PathBuf,
        #[relaxed] config: NetworkPlotConfig,
        fit: bool,
        label: Option<Template>,
        highlight: &[usize],
    ) -> anyhow::Result<()> {
        let n = net.nodes_count();
        if n == 0 {
            return Err(anyhow::Error::msg("Empty Network"));
        }
        let max_level = net.nodes().map(|n| n.lock().level()).max().unwrap_or(0);

        let mut surf =
            cairo::SvgSurface::new::<&std::path::Path>(config.width, config.height, None)?;
        let ctx = cairo::Context::new(&mut surf)?;
        ctx.set_line_width(1.0);
        ctx.set_font_size(config.fontsize);
        ctx.set_font_face(&config.fontface);

        let mut twidth = 0.0;
        let labels = if let Some(templ) = label {
            net.nodes_rev()
                .map(|n| n.lock().render(&templ))
                .collect::<anyhow::Result<Vec<String>>>()?
        } else {
            net.nodes_rev().map(|_| String::new()).collect()
        };
        calc_text_width(&labels, &ctx, &mut twidth)?;
        let mut delx = config.delta_x;
        let mut dely = config.delta_y;

        let mut width = delx * max_level as f64 + 2.0 * config.radius + config.offset + twidth;
        let mut height = dely * (n + 1) as f64 + 2.0 * config.radius;

        let mut surf = if fit {
            delx = (config.width - 2.0 * config.radius - twidth) / (max_level + 1) as f64;
            dely = (config.height - 2.0 * config.radius) / (n + 2) as f64;
            width = config.width;
            height = config.height;
            cairo::SvgSurface::new(config.width, config.height, Some(outfile))?
        } else {
            cairo::SvgSurface::new(width, height, Some(outfile))?
        };

        let ctx = cairo::Context::new(&mut surf)?;
        ctx.set_line_width(1.0);
        ctx.set_font_size(config.fontsize);
        ctx.set_font_face(&config.fontface);

        let offset = width - twidth;

        net.nodes_rev()
            .zip(labels)
            .try_for_each(|(n, l)| -> cairo::Result<()> {
                let n = n.lock();
                let y = height - (n.index() + 1) as f64 * dely;
                let x = n.level() as f64 * delx + delx / 2.0;

                ctx.set_source_rgb(0.5, 0.5, 1.0);
                if let RSome(o) = n.output() {
                    let o = o.lock();
                    let yo = height - (o.index() + 1) as f64 * dely;
                    let xo = o.level() as f64 * delx + delx / 2.0;
                    let dx = xo - x;
                    let dy = yo - y;
                    let l = (dx.powi(2) + dy.powi(2)).sqrt();
                    let (ux, uy) = (dx / l, dy / l);
                    let (sx, sy) = (x + ux * config.radius * 1.4, y + uy * config.radius * 1.4);
                    let (ex, ey) = (xo - ux * config.radius * 1.4, yo - uy * config.radius * 1.4);
                    ctx.move_to(sx, sy);
                    ctx.line_to(ex, ey);
                    ctx.stroke()?;
                    let (asx, asy) = (ex - ux * config.radius, ey - uy * config.radius);
                    let (aex, aey) = (xo - ux * config.radius, yo - uy * config.radius);
                    ctx.move_to(
                        asx + uy * config.radius * 0.5,
                        asy - ux * config.radius * 0.5,
                    );
                    ctx.line_to(aex, aey);
                    ctx.line_to(
                        asx - uy * config.radius * 0.5,
                        asy + ux * config.radius * 0.5,
                    );
                    ctx.line_to(asx + ux, asy + uy);
                    ctx.fill()?;
                    ctx.stroke()?;
                }
                if highlight.contains(&n.index()) {
                    ctx.set_source_rgb(1.0, 0.5, 0.5);
                } else {
                    ctx.set_source_rgb(0.5, 0.5, 1.0);
                }
                ctx.move_to(x + config.radius, y);
                ctx.arc(x, y, config.radius, 0.0, 2.0 * 3.1416);
                ctx.fill()?;
                ctx.stroke()?;
                ctx.move_to(offset, y);
                ctx.show_text(&l)
            })?;

        Ok(())
    }

    /// Create a SVG file with the given network structure
    #[network_func(config = NetworkPlotConfig::default(), fit = false, highlight = Vec::new())]
    fn table_to_svg(
        net: &mut Network,
        outfile: PathBuf,
        table: Option<PathBuf>,
        template: Option<String>,
        #[relaxed] config: NetworkPlotConfig,
        fit: bool,
        highlight: &[String],
    ) -> anyhow::Result<()> {
        let table = match (table, template) {
            (Some(t), None) => nadi_core::table::Table::from_file(t)?,
            (None, Some(t)) => nadi_core::table::Table::from_str(&t)?,
            (Some(_), Some(_)) => return Err(anyhow::Error::msg("table and template both given")),
            (None, None) => return Err(anyhow::Error::msg("neither table nor template given")),
        };
        let highlight: Vec<usize> = highlight
            .iter()
            .map(|n| {
                net.node_by_name(n)
                    .context("Node not found")
                    .map(|n| n.lock().index())
            })
            .collect::<anyhow::Result<Vec<usize>>>()?;
        export_svg_table(net, table, outfile, config, fit, &highlight)
    }
}

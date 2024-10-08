use nadi_core::nadi_plugin::nadi_plugin;

mod plots;
mod timeseries;

#[nadi_plugin]
mod graphics {
    use super::plots::*;
    use super::timeseries;
    use abi_stable::std_types::{RSome, Tuple2};
    use nadi_core::attrs::{FromAttribute, FromAttributeRelaxed};
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::table::ColumnAlign;
    use nadi_core::table::Table;
    use nadi_core::{AttrMap, Attribute, Network};
    use polars::prelude::*;
    use std::path::PathBuf;
    use std::str::FromStr;
    use string_template_plus::Template;

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
                    .set_attr(&a, Attribute::Integer(nas))
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
        date_col: String,
        label: Template,
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
    #[network_func(config = NetworkPlotConfig::default(), fit = false)]
    fn export_svg(
        net: &mut Network,
        outfile: PathBuf,
        #[relaxed] config: NetworkPlotConfig,
        fit: bool,
        label: Option<Template>,
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
        ctx.set_source_rgb(0.5, 0.5, 1.0);

        let offset = width - twidth;

        net.nodes_rev()
            .zip(labels)
            .try_for_each(|(n, l)| -> cairo::Result<()> {
                let n = n.lock();
                let y = height - (n.index() + 1) as f64 * dely;
                let x = n.level() as f64 * delx + delx / 2.0;

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
    #[network_func(config = NetworkPlotConfig::default(), fit = false)]
    fn table_to_svg(
        net: &mut Network,
        table: Option<PathBuf>,
        template: Option<String>,
        outfile: PathBuf,
        #[relaxed] config: NetworkPlotConfig,
        fit: bool,
    ) -> anyhow::Result<()> {
        let table = match (table, template) {
            (Some(t), None) => nadi_core::table::Table::from_file(t)?,
            (None, Some(t)) => nadi_core::table::Table::from_str(&t)?,
            (Some(_), Some(_)) => return Err(anyhow::Error::msg("table and template both given")),
            (None, None) => return Err(anyhow::Error::msg("neither table nor template given")),
        };
        export_svg_table(net, table, outfile, config, fit)
    }
}

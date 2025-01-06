use crate::plots::*;
use abi_stable::std_types::RSome;
use nadi_core::prelude::*;
use nadi_core::string_template::Template;
use polars::prelude::*;
use std::path::PathBuf;

#[derive(Debug, Default)]
pub struct Block {
    pub start: i64,
    pub end: i64,
}

#[derive(Debug, Default)]
pub struct Blocks {
    pub blocks: Vec<Vec<Block>>,
    pub _start: i64,
    pub _end: i64,
}

/// Create a SVG file with the given network structure
pub fn csv_data_blocks_svg(
    net: &Network,
    csv: PathBuf,
    outfile: PathBuf,
    date_col: String,
    label: Template,
    config: NetworkPlotConfig,
    blocks_width: f64,
    fit: bool,
) -> anyhow::Result<()> {
    let n = net.nodes_count();
    if n == 0 {
        return Err(anyhow::Error::msg("Empty Network"));
    }

    let mut surf = cairo::SvgSurface::new::<&std::path::Path>(config.width, config.height, None)?;
    let ctx = cairo::Context::new(&mut surf)?;
    ctx.set_line_width(1.0);
    ctx.set_font_size(config.fontsize);
    ctx.set_font_face(&config.fontface);

    let mut twidth = 0.0;
    let labels = net
        .nodes_rev()
        .map(|n| n.lock().render(&label))
        .collect::<anyhow::Result<Vec<String>>>()?;
    calc_text_width(&labels, &ctx, &mut twidth)?;

    // for the data blocks
    twidth += blocks_width + 2.0 * config.offset;

    let mut delx = config.delta_x;
    let mut dely = config.delta_y;

    let max_level = net.nodes().map(|n| n.lock().level()).max().unwrap_or(0);

    let mut width = delx * max_level as f64 + 2.0 * config.radius + twidth;
    let mut height = dely * (n + 2) as f64 + 2.0 * config.radius;

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
    ctx.set_source_rgb(0.35, 0.35, 0.6);

    let block_start = width - blocks_width - config.offset / 2.0;
    let label_start = width - twidth + config.offset;
    let blocks = csv_data_blocks(&net, csv, date_col)?;
    let blocks_min = blocks
        .blocks
        .iter()
        .map(|bs| bs.iter().map(|b| b.start).min().expect("No minimum"))
        .min()
        .expect("No minimum");
    let blocks_max = blocks
        .blocks
        .iter()
        .map(|bs| bs.iter().map(|b| b.start).max().expect("No maximum"))
        .max()
        .expect("No maximum");
    let blocks_diff = blocks_max - blocks_min;
    let blocks: Vec<Vec<(f64, f64)>> = blocks
        .blocks
        .into_iter()
        .rev()
        .map(|bs| {
            bs.into_iter()
                .map(|b| {
                    (
                        (b.start - blocks_min) as f64 / blocks_diff as f64,
                        (b.end - blocks_min) as f64 / blocks_diff as f64,
                    )
                })
                .map(|(s, e)| {
                    (
                        block_start + blocks_width * s,
                        block_start + blocks_width * e,
                    )
                })
                .collect()
        })
        .collect();

    net.nodes_rev().zip(labels).zip(blocks).try_for_each(
        |((n, l), blks)| -> cairo::Result<()> {
            let n = n.lock();
            let y = height - (n.index() + 1) as f64 * dely;
            let x = n.level() as f64 * delx + config.offset / 2.0;

            if let RSome(o) = n.output() {
                let o = o.lock();
                let yo = height - (o.index() + 1) as f64 * dely;
                let xo = o.level() as f64 * delx + config.offset / 2.0;
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
            ctx.move_to(label_start, y);
            ctx.show_text(&l)?;
            ctx.set_source_rgb(0.35, 0.65, 0.3);
            draw_blocks(&ctx, block_start, y, dely / 3.0, &blks)?;
            ctx.set_source_rgb(0.35, 0.35, 0.6);
            Ok(())
        },
    )?;

    Ok(())
}

pub fn csv_data_blocks(net: &Network, file: PathBuf, date_col: String) -> anyhow::Result<Blocks> {
    let columns: Vec<&str> = net.node_names().collect();
    let df = CsvReadOptions::default()
        .map_parse_options(|parse_options| parse_options.with_try_parse_dates(true))
        .try_into_reader_with_file_path(Some(file.into()))?
        .finish()?;

    let dfc = df
        .lazy()
        .with_columns([cols(&columns).is_null()])
        .collect()?;

    let mut blocks = Blocks::default();
    for column in &columns {
        let dates = dfc
            .clone()
            .lazy()
            .select([col(&date_col), col(column).alias("is_na")])
            .with_columns([col("is_na")
                .neq(col("is_na").shift(1.into()))
                .fill_null(true)
                .alias("block")])
            .with_columns([col("block").cast(DataType::Int64).cum_sum(false)])
            .filter(col("is_na").eq(lit(false)))
            .group_by([col("block")])
            .agg([
                col("is_na").first(),
                col(&date_col)
                    .first()
                    .dt()
                    .timestamp(TimeUnit::Milliseconds)
                    .alias("start"),
                col(&date_col)
                    .last()
                    .dt()
                    .timestamp(TimeUnit::Milliseconds)
                    .alias("end"),
            ])
            // .with_columns([(col("end") - col("start") + lit(Duration::parse("1d")))
            //     .dt()
            //     .total_days()
            //     .alias("days")])
            // .sort(
            //     ["block"],
            //     SortMultipleOptions::default()
            //         .with_order_descending(false)
            //         .with_nulls_last(true),
            // )
            .collect()
            .expect("Date blocks collection failed");
        let mut col_iters = dates
            .columns(["start", "end"])?
            .iter()
            .map(|s| Ok(s.i64()?.into_iter()))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut blks = Vec::<Block>::new();
        for _ in 0..dates.height() {
            let mut blocks_data = [0; 2];
            for (i, col) in col_iters.iter_mut().enumerate() {
                let val = col.next().expect("Next 1").expect("Next 2");
                blocks_data[i] = val;
            }
            blks.push(Block {
                start: blocks_data[0],
                end: blocks_data[1],
            });
        }
        blocks.blocks.push(blks);
    }
    Ok(blocks)
}

fn draw_blocks(
    ctx: &cairo::Context,
    _x0: f64,
    y0: f64,
    a: f64,
    blocks: &[(f64, f64)],
) -> cairo::Result<()> {
    for &(x1, x2) in blocks {
        let l = x2 - x1;
        let a = f64::min(a, l / 3.0);
        ctx.move_to(x1, y0);
        ctx.line_to(x2, y0);
        ctx.stroke()?;
        ctx.move_to(x1 + a * 0.4, y0 + a * 0.4);
        ctx.line_to(x1, y0);
        ctx.line_to(x1 + a * 0.4, y0 - a * 0.4);
        ctx.stroke()?;
        ctx.move_to(x2 - a * 0.4, y0 + a * 0.4);
        ctx.line_to(x2, y0);
        ctx.line_to(x2 - a * 0.4, y0 - a * 0.4);
        ctx.stroke()?;
    }
    Ok(())
}

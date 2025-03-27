use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod svg {
    use nadi_core::abi_stable::std_types::RSome;
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::prelude::*;
    use nadi_core::string_template::Template;
    use std::path::Path;
    use svg::node::element::*;
    use svg::Document;

    /// Exports the network as a svg
    #[network_func(
	label = Template::parse_template("{_NAME}").unwrap(),
    )]
    fn svg_save(
        net: &mut Network,
        outfile: &Path,
        label: Template,
        width: Option<u64>,
        height: Option<u64>,
    ) -> anyhow::Result<()> {
        let count = net.nodes_count();
        let level = net
            .nodes()
            .map(|n| n.lock().level())
            .max()
            .unwrap_or_default();

        let mut nodes = Group::new();
        let mut edges = Group::new();
        for node in net.nodes() {
            let n = node.lock();
            let x = n.level() * 20 + 10;
            let y = (count - n.index()) * 20 + 10;
            let lab = n
                .render(&label)
                .unwrap_or_else(|_| label.original().to_string());
            nodes = nodes
                .add(
                    Circle::new()
                        .set("cx", x)
                        .set("cy", y)
                        .set("r", 5)
                        .set("fill", "blue"),
                )
                .add(
                    Text::new(lab)
                        .set("x", 20 * (level + 2))
                        .set("y", y)
                        .set("text-anchor", "start"),
                );
            if let RSome(out) = n.output() {
                let o = out.lock();
                let xo = o.level() * 20 + 10;
                let yo = (count - o.index()) * 20 + 10;
                edges = edges.add(
                    Line::new()
                        .set("x1", x)
                        .set("y1", y)
                        .set("x2", xo)
                        .set("y2", yo)
                        .set("style", "stroke:red;stroke-width:2"),
                );
            }
        }
        let doc = Document::new()
            .set(
                "viewBox",
                (
                    0,
                    0,
                    width.unwrap_or(20 + 20 * level),
                    height.unwrap_or(20 + 20 * count as u64),
                ),
            )
            .add(edges)
            .add(nodes);
        svg::save(outfile, &doc)?;
        Ok(())
    }
}

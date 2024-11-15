use nadi_core::nadi_plugin::nadi_plugin;

#[nadi_plugin]
mod html {
    use nadi_core::nadi_plugin::network_func;
    use nadi_core::prelude::*;
    use nadi_core::string_template::Template;
    use nadi_core::abi_stable::std_types::RSome;
    use std::{fs::File, io::Write, path::Path};

    /// Exports the network as a HTML map
    #[network_func(
	pagetitle="NADI Network",
	nodetitle = Template::parse_template("{_NAME}").unwrap(),
	connections = true
    )]
    fn export_map(net: &mut Network, outfile: &Path, template: Template, pagetitle: &str, nodetitle: Template, connections: bool) -> anyhow::Result<()> {
	let mut file = File::create(outfile)?;
	writeln!(file, "<!DOCTYPE html>\n<html>\n<title>{pagetitle}</title>")?;
	writeln!(file, "
<head>
        <!-- You can Include your CSS here-->
        <style>
            /* Sidebar Div */
            div.network {{
                position: fixed;
                left: 0;
                color: #fff;
                width: 250px;
                padding-left: 20px;
                height: 100vh;
                background-color: #055e21;
                border-top-right-radius: 20px;
            }}

            div.contents {{
              margin-left: 300px; /* Same as the width of the sidebar */
              padding: 5px 5px;
            }}
        </style>
</head>
")?;

	writeln!(file, "<body>")?;
	if connections {
	    write_nodes_svg(&mut file, &net)?;
	} else {
	    write_nodes_list(&mut file, &net)?;
	}
	
	writeln!(file, "<div class=\"contents\">")?;
	for node in net.nodes_rev(){
	    let n = node.lock();
	    writeln!(file, "<h2 id=\"{}\">{}</h2>", n.name(), n.render(&nodetitle)?)?;
	    writeln!(file, "{}", n.render(&template)?)?;
	}
	writeln!(file, "</div>")?;
	

	writeln!(file, "</body>\n</html>")?;
	Ok(())
    }

    fn write_nodes_list(file: &mut File, net: &Network) -> anyhow::Result<()> {
	writeln!(file, "<div class=\"network\"><ol>")?;
	for node in net.nodes(){
	    writeln!(file, "<li>{}</li>", node.lock().name())?;    
	}
	writeln!(file, "</ol></div>")?;
	Ok(())
    }

    fn write_nodes_svg(file: &mut File, net: &Network) -> anyhow::Result<()> {
	let c = net.nodes_count();
	writeln!(file, "<div class=\"network\"><svg  width=\"300\" height=\"{}\">", (c + 1) * 20)?;
	for node in net.nodes(){
	    let n = node.lock();
	    let x = n.level() * 20 + 15;
	    let y = (c - n.index()) * 20;
	    if let RSome(out) = n.output() {
		let o = out.lock();
		let xo = o.level() * 20 + 15;
		let yo = (c - o.index()) * 20;
		writeln!(file, "<line x1=\"{x}\" y1=\"{y}\" x2=\"{xo}\" y2=\"{yo}\" style=\"stroke:red;stroke-width:2\" />")?;
	    }
	    writeln!(file, "<circle cx=\"{x}\" cy=\"{y}\" r=\"5\" fill=\"yellow\" />")?;
	    writeln!(file, "<a href=\"#{0}\"><text fill=\"#8888ff\" x=\"100\" y=\"{y}\">{0}</text></a>", n.name())?;
	}
	writeln!(file, "</svg></div>")?;
	Ok(())
    }
}

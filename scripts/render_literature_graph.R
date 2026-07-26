#!/usr/bin/env Rscript

usage <- function() {
  cat(paste(
    "Usage:",
    "  Rscript scripts/render_literature_graph.R --input graph.json --output graph.html",
    "",
    "Options:",
    "  --input PATH   Literature graph JSON from build_literature_graph.R.",
    "  --output PATH  Output standalone HTML file.",
    "  -h, --help     Show this help message.",
    "",
    "The generated HTML loads Cytoscape.js 3.30.4 from unpkg.com.",
    sep = "\n"
  ))
}

parse_args <- function(args) {
  out <- list(help = FALSE, input = NULL, output = NULL)
  i <- 1L
  while (i <= length(args)) {
    key <- args[[i]]
    if (identical(key, "-h") || identical(key, "--help")) {
      out$help <- TRUE
      i <- i + 1L
      next
    }
    if (i == length(args)) stop("Missing value for ", key, call. = FALSE)
    value <- args[[i + 1L]]
    if (identical(key, "--input")) out$input <- value else if (identical(key, "--output")) out$output <- value else stop("Unknown argument: ", key, call. = FALSE)
    i <- i + 2L
  }
  out
}

parsed <- parse_args(commandArgs(trailingOnly = TRUE))
if (isTRUE(parsed$help)) {
  usage()
  quit(status = 0L)
}
if (is.null(parsed$input) || is.null(parsed$output)) stop("--input and --output are required.", call. = FALSE)
if (!file.exists(parsed$input)) stop("Graph JSON not found: ", parsed$input, call. = FALSE)

graph <- jsonlite::read_json(parsed$input, simplifyVector = FALSE)
if (!is.list(graph$nodes) || !is.list(graph$edges)) stop("Input must contain `nodes` and `edges` arrays.", call. = FALSE)
graph_json <- jsonlite::toJSON(graph, auto_unbox = TRUE, null = "null", dataframe = "rows")
graph_json <- gsub("</", "<\\\\/", graph_json, fixed = TRUE)

html <- paste0(
  "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>Literature Relationship Graph</title>",
  "<script src=\"https://unpkg.com/cytoscape@3.30.4/dist/cytoscape.min.js\"></script><style>",
  "*{box-sizing:border-box}body{margin:0;background:#f4f0e8;color:#17231f;font:14px Georgia,serif}header{padding:16px 22px;background:#173b35;color:#f7f0dc;display:flex;gap:16px;align-items:center}h1{font-size:20px;margin:0;letter-spacing:.02em}#search{margin-left:auto;width:260px;padding:7px 9px;border:0;border-radius:3px;font:13px Georgia,serif}.layout{border:1px solid #b2b8a6;background:#f7f0dc;padding:6px 9px;border-radius:3px;cursor:pointer;font:13px Georgia,serif}main{display:grid;grid-template-columns:minmax(0,1fr) 310px;height:calc(100vh - 58px)}#cy{min-height:500px;background:radial-gradient(circle at 20% 20%,#fffdf7,#e7eee5)}aside{border-left:1px solid #bdc7be;background:#fffdf7;padding:18px;overflow:auto}aside h2{font-size:16px;margin:0 0 10px;color:#173b35}.muted{color:#64736a;line-height:1.45}dl{margin:0}dt{font-weight:bold;color:#31584e;margin-top:12px}dd{margin:3px 0;white-space:pre-wrap;line-height:1.4}@media(max-width:780px){main{grid-template-columns:1fr;grid-template-rows:62vh auto}aside{border-left:0;border-top:1px solid #bdc7be}}
  </style></head><body><header><h1>Literature Relationship Graph</h1><button class=\"layout\" id=\"hierarchy\">Hierarchy</button><button class=\"layout\" id=\"network\">Network</button><input id=\"search\" placeholder=\"Filter ids or titles\"></header><main><div id=\"cy\"></div><aside><h2>Graph Details</h2><div id=\"detail\" class=\"muted\">Select a paper or relationship.</div></aside></main><script>",
  "const graph=", graph_json, ";function edgeClass(edge){const token=(String(edge.relationship||'')+' '+String(edge.anchor_role||'')).toLowerCase();if(token.includes('builds_on')||token.includes('foundation'))return'foundation';if(token.includes('extends'))return'extension';if(token.includes('compare')||token.includes('contrast'))return'comparison';return'context'}const nodes=graph.nodes.map(n=>({data:{id:n.id,label:n.title||n.ref_id,ref_id:n.ref_id,node_type:n.node_type,depth:n.depth,is_root:n.is_root,summary:n.summary,theoretical_mechanism:n.theoretical_mechanism,github_urls:n.github_urls},classes:n.is_root?'root':''}));const edges=graph.edges.map(e=>({data:{id:e.id,source:e.source,target:e.target,anchor_role:e.anchor_role,relationship:e.relationship,confidence:e.confidence,reason:e.reason},classes:edgeClass(e)}));",
  "const cy=cytoscape({container:document.getElementById('cy'),elements:{nodes,edges},style:[{selector:'node',style:{'label':'data(label)','font-size':10,'text-wrap':'wrap','text-max-width':125,'background-color':'#4c7a69','color':'#17231f','text-outline-color':'#fffdf7','text-outline-width':2,'width':28,'height':28}},{selector:'node[node_type = \"external\"]',style:{'background-color':'#9aa29b','shape':'diamond'}},{selector:'node.root',style:{'background-color':'#c77736','width':36,'height':36}},{selector:'edge',style:{'width':1.4,'target-arrow-shape':'triangle','curve-style':'bezier'}},{selector:'edge.foundation',style:{'line-color':'#31584e','target-arrow-color':'#31584e','line-style':'solid'}},{selector:'edge.extension',style:{'line-color':'#c77736','target-arrow-color':'#c77736','line-style':'dashed'}},{selector:'edge.comparison',style:{'line-color':'#a6863d','target-arrow-color':'#a6863d','line-style':'dotted'}},{selector:'edge.context',style:{'line-color':'#839388','target-arrow-color':'#839388','line-style':'solid'}}],layout:{name:'breadthfirst',directed:true,padding:40,spacingFactor:1.25}});",
  "const detail=document.getElementById('detail');function text(v){return v===null||v===undefined||v===''?'Not recorded':v}function esc(v){return String(text(v)).replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]))}function show(rows){detail.innerHTML='<dl>'+rows.map(([k,v])=>'<dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd>').join('')+'</dl>'}cy.on('tap','node',e=>{const n=e.target.data();show([['Reference id',n.ref_id],['Title',n.label],['Summary',n.summary],['Theoretical mechanism',n.theoretical_mechanism],['GitHub URLs',n.github_urls],['Storage',n.node_type],['Depth',n.depth],['Root',n.is_root?'yes':'no']])});cy.on('tap','edge',e=>{const d=e.target.data();show([['Relationship',d.relationship],['Anchor role',d.anchor_role],['Confidence',d.confidence],['Reason',d.reason]])});document.getElementById('hierarchy').onclick=()=>cy.layout({name:'breadthfirst',directed:true,padding:40,spacingFactor:1.25}).run();document.getElementById('network').onclick=()=>cy.layout({name:'cose',padding:40,animate:false,idealEdgeLength:120}).run();document.getElementById('search').oninput=e=>{const q=e.target.value.trim().toLowerCase();cy.nodes().forEach(n=>{const visible=!q||String(n.data('ref_id')).toLowerCase().includes(q)||String(n.data('label')).toLowerCase().includes(q);n.style('display',visible?'element':'none')});cy.edges().forEach(edge=>edge.style('display',edge.source().style('display')==='none'||edge.target().style('display')==='none'?'none':'element'))};const roots=Array.isArray(graph.meta.root_ref_ids)?graph.meta.root_ref_ids:[graph.meta.root_ref_ids];show([['Roots',roots.filter(Boolean).join(', ')],['Nodes',graph.meta.returned_nodes],['Edges',graph.meta.returned_edges],['External anchors',graph.meta.external_nodes],['Truncated nodes',graph.meta.truncated_nodes]]);</script></body></html>"
)

legend_markup <- paste0(
  "<style>#edge-legend{width:29px;height:29px;padding:0;border:1px solid #b2b8a6;border-radius:50%;background:#f7f0dc;color:#173b35;cursor:pointer;font:700 16px Georgia,serif;line-height:1}#edge-legend-modal[hidden]{display:none}#edge-legend-modal{position:fixed;inset:0;z-index:10;display:grid;place-items:center;padding:20px;background:rgba(23,35,31,.48)}.legend-card{position:relative;width:min(460px,100%);padding:22px;background:#fffdf7;border:1px solid #bdc7be;box-shadow:0 18px 50px rgba(0,0,0,.28)}.legend-card h2{margin:0 36px 14px 0;color:#173b35}.legend-card p{line-height:1.45}.legend-close{position:absolute;top:10px;right:12px;border:0;background:transparent;color:#31584e;font:22px Georgia,serif;cursor:pointer}.legend-row{display:flex;align-items:center;gap:10px;margin:10px 0}.edge-sample{position:relative;width:56px;border-top:3px solid #31584e}.edge-sample:after{content:'';position:absolute;right:-1px;top:-6px;border-left:8px solid #31584e;border-top:5px solid transparent;border-bottom:5px solid transparent}.edge-sample.extension{border-top-color:#c77736;border-top-style:dashed}.edge-sample.extension:after{border-left-color:#c77736}.edge-sample.comparison{border-top-color:#a6863d;border-top-style:dotted}.edge-sample.comparison:after{border-left-color:#a6863d}.edge-sample.context{border-top-color:#839388}.edge-sample.context:after{border-left-color:#839388}</style>",
  "<script>const networkButton=document.getElementById('network');networkButton.insertAdjacentHTML('afterend','<button id=\"edge-legend\" type=\"button\" aria-label=\"Show edge legend\" title=\"Edge legend\">&#9432;</button>');document.body.insertAdjacentHTML('beforeend','<div id=\"edge-legend-modal\" hidden><section class=\"legend-card\" role=\"dialog\" aria-modal=\"true\" aria-labelledby=\"edge-legend-title\"><button class=\"legend-close\" id=\"edge-legend-close\" type=\"button\" aria-label=\"Close edge legend\">&times;</button><h2 id=\"edge-legend-title\">Edge styles</h2><div class=\"legend-row\"><span class=\"edge-sample\"></span><span><strong>Foundation</strong>: builds on or cites a foundation.</span></div><div class=\"legend-row\"><span class=\"edge-sample extension\"></span><span><strong>Extension</strong>: extends prior work.</span></div><div class=\"legend-row\"><span class=\"edge-sample comparison\"></span><span><strong>Comparison</strong>: compares or contrasts methods.</span></div><div class=\"legend-row\"><span class=\"edge-sample context\"></span><span><strong>Context</strong>: other contextual relationship.</span></div><p>Arrows point from the citing paper to its anchored reference.</p></section></div>');const legendModal=document.getElementById('edge-legend-modal');const closeLegend=()=>{legendModal.hidden=true};document.getElementById('edge-legend').onclick=()=>{legendModal.hidden=false};document.getElementById('edge-legend-close').onclick=closeLegend;legendModal.onclick=e=>{if(e.target===legendModal)closeLegend()};document.addEventListener('keydown',e=>{if(e.key==='Escape')closeLegend()});const graphNodesById=new Map(graph.nodes.map(node=>[node.id,node]));const fullTitle=node=>{const source=graphNodesById.get(node.id);return source&&source.title?source.title:node.ref_id};const setCanvasLabels=network=>{cy.nodes().forEach(node=>{const data=node.data();node.data('label',network?data.ref_id:fullTitle(data))})};cy.on('tap','node',e=>{const node=e.target.data();show([['Reference id',node.ref_id],['Title',fullTitle(node)],['Summary',node.summary],['Theoretical mechanism',node.theoretical_mechanism],['GitHub URLs',node.github_urls],['Storage',node.node_type],['Depth',node.depth],['Root',node.is_root?'yes':'no']])});document.getElementById('hierarchy').onclick=()=>{setCanvasLabels(false);cy.layout({name:'breadthfirst',directed:true,padding:40,spacingFactor:1.25}).run()};networkButton.onclick=()=>{setCanvasLabels(true);cy.layout({name:'cose',padding:40,animate:false,idealEdgeLength:120}).run()};</script>"
)
network_layout_markup <- "<script>const networkLayoutButton=document.getElementById('network');const hierarchyLayoutButton=document.getElementById('hierarchy');const setNetworkCanvas=network=>{setCanvasLabels(network);cy.style().selector('node').style({'width':network?20:28,'height':network?20:28,'text-valign':network?'bottom':'center','text-margin-y':network?8:0}).selector('node.root').style({'width':network?26:36,'height':network?26:36}).update()};hierarchyLayoutButton.onclick=()=>{setNetworkCanvas(false);cy.layout({name:'breadthfirst',directed:true,padding:40,spacingFactor:1.25}).run()};networkLayoutButton.onclick=()=>{setNetworkCanvas(true);cy.layout({name:'cose',padding:70,animate:false,nodeDimensionsIncludeLabels:true,avoidOverlap:true,idealEdgeLength:()=>180,nodeRepulsion:()=>9000}).run()};</script>"
legend_markup <- paste0(legend_markup, network_layout_markup)
html <- sub("</body>", paste0(legend_markup, "</body>"), html, fixed = TRUE)

dir.create(dirname(parsed$output), recursive = TRUE, showWarnings = FALSE)
writeLines(html, parsed$output, useBytes = TRUE)
writeLines(jsonlite::toJSON(list(status = "ok", output = normalizePath(parsed$output, winslash = "/", mustWork = FALSE)), auto_unbox = TRUE), con = stdout())

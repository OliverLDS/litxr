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
  "const graph=", graph_json, ";const nodes=graph.nodes.map(n=>({data:{id:n.id,label:n.title||n.ref_id,ref_id:n.ref_id,node_type:n.node_type,depth:n.depth,is_root:n.is_root},classes:n.is_root?'root':''}));const edges=graph.edges.map(e=>({data:{id:e.id,source:e.source,target:e.target,anchor_role:e.anchor_role,relationship:e.relationship,confidence:e.confidence,reason:e.reason}}));",
  "const cy=cytoscape({container:document.getElementById('cy'),elements:{nodes,edges},style:[{selector:'node',style:{'label':'data(label)','font-size':10,'text-wrap':'wrap','text-max-width':125,'background-color':'#4c7a69','color':'#17231f','text-outline-color':'#fffdf7','text-outline-width':2,'width':28,'height':28}},{selector:'node[node_type = \"external\"]',style:{'background-color':'#9aa29b','shape':'diamond'}},{selector:'node.root',style:{'background-color':'#c77736','width':36,'height':36}},{selector:'edge',style:{'width':1.4,'line-color':'#839388','target-arrow-color':'#839388','target-arrow-shape':'triangle','curve-style':'bezier','label':'data(relationship)','font-size':8,'text-rotation':'autorotate','color':'#536158'}}],layout:{name:'breadthfirst',directed:true,padding:40,spacingFactor:1.25}});",
  "const detail=document.getElementById('detail');function text(v){return v===null||v===undefined||v===''?'Not recorded':v}function esc(v){return String(text(v)).replace(/[&<>\"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]))}function show(rows){detail.innerHTML='<dl>'+rows.map(([k,v])=>'<dt>'+esc(k)+'</dt><dd>'+esc(v)+'</dd>').join('')+'</dl>'}cy.on('tap','node',e=>{const n=e.target.data();show([['Reference id',n.ref_id],['Title',n.label],['Storage',n.node_type],['Depth',n.depth],['Root',n.is_root?'yes':'no']])});cy.on('tap','edge',e=>{const d=e.target.data();show([['Relationship',d.relationship],['Anchor role',d.anchor_role],['Confidence',d.confidence],['Reason',d.reason]])});document.getElementById('hierarchy').onclick=()=>cy.layout({name:'breadthfirst',directed:true,padding:40,spacingFactor:1.25}).run();document.getElementById('network').onclick=()=>cy.layout({name:'cose',padding:40,animate:false,idealEdgeLength:120}).run();document.getElementById('search').oninput=e=>{const q=e.target.value.trim().toLowerCase();cy.nodes().forEach(n=>{const visible=!q||String(n.data('ref_id')).toLowerCase().includes(q)||String(n.data('label')).toLowerCase().includes(q);n.style('display',visible?'element':'none')});cy.edges().forEach(edge=>edge.style('display',edge.source().style('display')==='none'||edge.target().style('display')==='none'?'none':'element'))};const roots=Array.isArray(graph.meta.root_ref_ids)?graph.meta.root_ref_ids:[graph.meta.root_ref_ids];show([['Roots',roots.filter(Boolean).join(', ')],['Nodes',graph.meta.returned_nodes],['Edges',graph.meta.returned_edges],['External anchors',graph.meta.external_nodes],['Truncated nodes',graph.meta.truncated_nodes]]);</script></body></html>"
)

dir.create(dirname(parsed$output), recursive = TRUE, showWarnings = FALSE)
writeLines(html, parsed$output, useBytes = TRUE)
writeLines(jsonlite::toJSON(list(status = "ok", output = normalizePath(parsed$output, winslash = "/", mustWork = FALSE)), auto_unbox = TRUE), con = stdout())

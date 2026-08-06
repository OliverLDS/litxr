#!/usr/bin/env node

const fs = require("node:fs");
const http = require("node:http");
const os = require("node:os");
const path = require("node:path");
const { execFile } = require("node:child_process");

function usage() {
  process.stdout.write(`Usage:
  node scripts/serve_literature_projects.js --projects-path PATH [--port PORT] [--no-browser]

Options:
  --projects-path PATH  Required human-managed projects directory.
  --port PORT           Optional localhost port. Default: 8787.
  --no-browser          Start without opening the browser.
  -h, --help            Show this help message.

The server binds only to 127.0.0.1. Stop it with Ctrl-C.
`);
}

function parseArgs(args) {
  const parsed = { projectsPath: "", port: 8787, browser: true };
  for (let i = 0; i < args.length; i += 1) {
    const key = args[i];
    if (key === "-h" || key === "--help") return { ...parsed, help: true };
    if (key === "--no-browser") {
      parsed.browser = false;
      continue;
    }
    if (i + 1 >= args.length) throw new Error(`Missing value for ${key}`);
    const value = args[++i];
    if (key === "--projects-path") parsed.projectsPath = value;
    else if (key === "--port") parsed.port = Number(value);
    else throw new Error(`Unknown argument: ${key}`);
  }
  return parsed;
}

function normalizeRefId(value) {
  const refId = String(value || "")
    .trim()
    .replace(/^(arxiv|doi|isbn):/i, "")
    .toLowerCase();
  const arxiv = /^\d{4}\.\d{4,5}$/.test(refId);
  const doi = /^10\.\d{4,9}\/\S+$/.test(refId);
  const isbn = /^(?:\d{9}[\dx]|\d{13})$/.test(refId.replace(/[- ]/g, ""));
  if (!arxiv && !doi && !isbn) throw new Error(`Unsupported bare reference id: ${refId}`);
  return isbn ? refId.replace(/[- ]/g, "") : refId;
}

function canonicalRefId(refId) {
  if (/^\d{4}\.\d{4,5}$/.test(refId)) return `arxiv:${refId}`;
  if (/^10\./.test(refId)) return `doi:${refId}`;
  return `isbn:${refId}`;
}

function projectIdFor(name, existingIds) {
  const base = String(name || "")
    .normalize("NFKD")
    .replace(/[^\x00-\x7F]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "") || "project";
  let candidate = base;
  let suffix = 2;
  while (existingIds.has(candidate)) candidate = `${base}_${suffix++}`;
  return candidate;
}

function validateProject(project, filePath) {
  const projectId = String(project.project_id || "");
  const name = String(project.name || "").trim();
  if (!/^[a-z0-9][a-z0-9_]*$/.test(projectId)) throw new Error(`Invalid project_id in ${filePath}`);
  if (path.basename(filePath) !== `${projectId}.json`) throw new Error(`Filename does not match project_id: ${filePath}`);
  if (!name) throw new Error(`Empty project name in ${filePath}`);
  const refIds = [...new Set((project.ref_ids || []).map(normalizeRefId))];
  return { schema_version: 1, project_id: projectId, name, ref_ids: refIds };
}

function referenceCachePath(projectsPath) {
  return path.join(projectsPath, "_ref_cache.json");
}

function validateReferenceCache(cache, filePath) {
  const rawEntries = cache && typeof cache.references === "object" && cache.references ? cache.references : {};
  const references = {};
  for (const [rawRefId, entry] of Object.entries(rawEntries)) {
    const refId = normalizeRefId(rawRefId);
    if (!entry || typeof entry !== "object") throw new Error(`Invalid cached reference in ${filePath}: ${refId}`);
    const title = String(entry.title || "").trim();
    const summary = String(entry.summary || "");
    if (!title || !summary) throw new Error(`Incomplete cached reference in ${filePath}: ${refId}`);
    references[refId] = { title, summary, has_digest: entry.has_digest === true, cached_at: String(entry.cached_at || "") };
  }
  return { schema_version: 1, references };
}

function readReferenceCache(projectsPath) {
  const filePath = referenceCachePath(projectsPath);
  if (!fs.existsSync(filePath)) return { schema_version: 1, references: {} };
  return validateReferenceCache(JSON.parse(fs.readFileSync(filePath, "utf8")), filePath);
}

function writeReferenceCache(projectsPath, cache) {
  const filePath = referenceCachePath(projectsPath);
  const clean = validateReferenceCache(cache, filePath);
  const temporaryPath = `${filePath}.tmp`;
  fs.writeFileSync(temporaryPath, `${JSON.stringify(clean, null, 2)}\n`);
  fs.renameSync(temporaryPath, filePath);
  return clean;
}

function readProjects(projectsPath) {
  return fs.readdirSync(projectsPath)
    .filter((name) => /^[a-z0-9][a-z0-9_]*\.json$/.test(name))
    .map((name) => {
      const filePath = path.join(projectsPath, name);
      return validateProject(JSON.parse(fs.readFileSync(filePath, "utf8")), filePath);
    })
    .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));
}

function projectPath(projectsPath, projectId) {
  if (!/^[a-z0-9][a-z0-9_]*$/.test(projectId)) throw new Error(`Invalid project_id: ${projectId}`);
  return path.join(projectsPath, `${projectId}.json`);
}

function readProject(projectsPath, projectId) {
  const filePath = projectPath(projectsPath, projectId);
  if (!fs.existsSync(filePath)) throw new Error(`Project not found: ${projectId}`);
  return validateProject(JSON.parse(fs.readFileSync(filePath, "utf8")), filePath);
}

function writeProject(projectsPath, project) {
  const filePath = projectPath(projectsPath, project.project_id);
  const clean = validateProject(project, filePath);
  const temporaryPath = `${filePath}.tmp`;
  fs.writeFileSync(temporaryPath, `${JSON.stringify(clean, null, 2)}\n`);
  fs.renameSync(temporaryPath, filePath);
  return clean;
}

function migrateProjectReferenceCaches(projectsPath) {
  let sharedCache = readReferenceCache(projectsPath);
  let cacheChanged = false;
  for (const name of fs.readdirSync(projectsPath).filter((entry) => /^[a-z0-9][a-z0-9_]*\.json$/.test(entry))) {
    const filePath = path.join(projectsPath, name);
    const rawProject = JSON.parse(fs.readFileSync(filePath, "utf8"));
    const legacyCache = rawProject.reference_cache && typeof rawProject.reference_cache === "object" ? rawProject.reference_cache : {};
    const project = validateProject(rawProject, filePath);
    for (const refId of project.ref_ids) {
      const entry = legacyCache[refId];
      if (sharedCache.references[refId] || !entry || typeof entry !== "object") continue;
      const title = String(entry.title || "").trim();
      const summary = String(entry.summary || "");
      if (!title || !summary) continue;
      sharedCache.references[refId] = { title, summary, has_digest: entry.has_digest === true, cached_at: String(entry.cached_at || "") };
      cacheChanged = true;
    }
    if (Object.hasOwn(rawProject, "reference_cache")) writeProject(projectsPath, project);
  }
  if (cacheChanged || !fs.existsSync(referenceCachePath(projectsPath))) sharedCache = writeReferenceCache(projectsPath, sharedCache);
  return sharedCache;
}

function projectForClient(project, cache) {
  const visibleCache = {};
  for (const refId of project.ref_ids) {
    const entry = cache.references[refId];
    if (entry) visibleCache[refId] = { title: entry.title, has_digest: entry.has_digest, cached_at: entry.cached_at };
  }
  return { ...project, reference_cache: visibleCache };
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      try {
        const text = Buffer.concat(chunks).toString("utf8").trim();
        resolve(text ? JSON.parse(text) : {});
      } catch (error) {
        reject(new Error("Request body must be valid JSON."));
      }
    });
    request.on("error", reject);
  });
}

function sendJson(response, value, status = 200) {
  response.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store" });
  response.end(JSON.stringify(value));
}

function run(command, args) {
  return new Promise((resolve, reject) => {
    execFile(command, args, { maxBuffer: 16 * 1024 * 1024 }, (error, stdout, stderr) => {
      if (error) reject(new Error(String(stderr || stdout || error.message).trim()));
      else resolve(stdout);
    });
  });
}

function summaryArgs(refId) {
  const kindFlag = /^\d{4}\./.test(refId) ? "--arxiv-id" : /^10\./.test(refId) ? "--doi" : "--isbn";
  return [kindFlag, refId];
}

function titleFromSummary(summary, refId) {
  const match = String(summary).match(/^title:\s*(.+)$/mi);
  return match ? match[1].trim() : refId;
}

async function hydrateReference(summaryScript, refId) {
  const summary = await run("/bin/zsh", [summaryScript, ...summaryArgs(refId)]);
  const hasDigest = /^digest_present:\s*true\s*$/mi.test(summary);
  return { title: titleFromSummary(summary, refId), summary, has_digest: hasDigest, cached_at: new Date().toISOString() };
}

const html = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>LitXR Project Library</title>
<style>:root{--ink:#17231f;--forest:#173b35;--paper:#fffdf7;--sand:#f4f0e8;--line:#bdc7be}*{box-sizing:border-box}body{margin:0;color:var(--ink);background:var(--sand);font:15px Georgia,serif}header{height:62px;padding:0 22px;display:flex;align-items:center;gap:12px;background:var(--forest);color:#f7f0dc}h1{font-size:20px;margin:0}header button{margin-left:auto}main{--detail-width:42%;height:calc(100vh - 62px);display:grid;grid-template-columns:260px minmax(320px,1fr) 6px minmax(320px,var(--detail-width))}main.projects-hidden{grid-template-columns:0 minmax(320px,1fr) 6px minmax(320px,var(--detail-width))}section,aside{min-width:0;overflow:auto;background:var(--paper);border-right:1px solid var(--line)}main.projects-hidden>section:first-child{visibility:hidden;overflow:hidden;border:0}aside{border-right:0}.panel-resizer{background:var(--line);cursor:col-resize;transition:background .15s}.panel-resizer:hover,.panel-resizer.dragging{background:#648c7a}.head{position:sticky;top:0;z-index:2;padding:16px;background:var(--paper);border-bottom:1px solid var(--line)}.head h2{margin:0 0 10px;font-size:17px;color:var(--forest)}button,input{font:inherit}button{cursor:pointer;border:1px solid #8ca095;background:#f7f0dc;color:var(--forest);padding:6px 9px;border-radius:3px}input{min-width:0;border:1px solid var(--line);padding:7px}.row,.actions{display:flex;gap:6px}.row input{flex:1}.actions{margin-top:10px}.actions button{font-size:12px}.list{padding:10px}.item{width:100%;text-align:left;padding:10px;border:0;border-bottom:1px solid #e1e5dc;background:transparent;border-radius:0}.item.active{background:#e7eee5}.item strong,.item small{display:block}.item strong{line-height:1.28}.digest-marker{display:inline-grid;place-items:center;width:13px;height:13px;margin-left:5px;border-radius:50%;background:#2f6b58;color:#fff;font:700 9px/1 ui-monospace,SFMono-Regular,Menlo,monospace;vertical-align:1px}.item small,.empty,.ref{color:#64736a}.empty{padding:22px;line-height:1.5}.detail{padding:20px}.detail-header{display:flex;align-items:flex-start;gap:10px}.detail h2{margin:0 0 5px;color:var(--forest);font-size:21px}.detail-body{line-height:1.55}.detail-body h1,.detail-body h2,.detail-body h3{color:var(--forest);margin:1.25em 0 .4em}.detail-body h1{font-size:21px}.detail-body h2{font-size:18px}.detail-body h3{font-size:16px}.detail-body p{margin:.55em 0}.detail-body ol,.detail-body ul{padding-left:1.35em}.ref{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}.icon{margin-left:auto;padding:4px 7px;font-size:16px;line-height:1}.detail-actions{margin-top:18px;display:flex;gap:8px}@media(max-width:900px){main,main.projects-hidden{display:block;height:auto}main>section:first-child{max-height:45vh}main.projects-hidden>section:first-child{display:none}.panel-resizer{display:none}aside{max-height:55vh;border-top:1px solid var(--line)}}</style></head>
<body><header><h1>LitXR Project Library</h1><button id="toggle-projects" type="button">Hide projects</button></header><main id="app-main"><section><div class="head"><h2>Projects</h2><div class="row"><input id="project-name" placeholder="New project name"><button id="add-project">Add</button></div><div class="actions"><button id="delete" disabled>Delete</button></div></div><div id="projects" class="list"></div></section><section><div class="head"><h2 id="refs-title">References</h2><div class="row"><input id="ref-id" placeholder="Bare arXiv ID, DOI, or ISBN"><button id="add-ref" disabled>Add</button></div><div class="actions"><button id="export" disabled>Export .bib</button></div></div><div id="refs" class="list"></div></section><div id="panel-resizer" class="panel-resizer" role="separator" aria-label="Resize reference and summary panels" aria-orientation="vertical"></div><aside><div id="detail" class="empty">Select a reference to view its summary.</div></aside></main>
<script>
const state={projects:[],projectId:null,refId:null};
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(url,options={}){const response=await fetch(url,{...options,headers:{'Content-Type':'application/json'}});const data=await response.json();if(!response.ok)throw new Error(data.error||('HTTP '+response.status));return data}
const selected=()=>state.projects.find(p=>p.project_id===state.projectId)||null;
function summaryHtml(summary){const out=[],paragraph=[];let list='';const flush=()=>{if(paragraph.length){out.push('<p>'+paragraph.join(' ')+'</p>');paragraph.length=0}};const close=()=>{if(list){out.push('</'+list+'>');list=''}};for(const raw of String(summary).split(/\\r?\\n/)){const line=raw.trim();if(!line){flush();close();continue}if(/^(ref_id|title|digest_present):/i.test(line)||line==='research_schema')continue;if(line==='abstract'){flush();close();out.push('<h3>Abstract</h3>');continue}const heading=line.match(/^(#{1,3})\\s+(.+)/);if(heading){flush();close();out.push('<h'+heading[1].length+'>'+esc(heading[2])+'</h'+heading[1].length+'>');continue}const ordered=line.match(/^\\d+\\.\\s+(.+)/);const bullet=line.match(/^[-*]\\s+(.+)/);if(ordered||bullet){flush();const type=ordered?'ol':'ul';if(list&&list!==type)close();if(!list){out.push('<'+type+'>');list=type}out.push('<li>'+esc((ordered||bullet)[1])+'</li>');continue}close();paragraph.push(esc(line))}flush();close();return out.join('')}
function render(){const projects=document.getElementById('projects');projects.innerHTML=state.projects.length?state.projects.map(p=>'<button class="item '+(p.project_id===state.projectId?'active':'')+'" data-project="'+p.project_id+'"><strong>'+esc(p.name)+'</strong><small>'+p.ref_ids.length+' references</small></button>').join(''):'<div class="empty">No projects yet.</div>';projects.querySelectorAll('[data-project]').forEach(b=>b.onclick=()=>{state.projectId=b.dataset.project;state.refId=null;render();document.getElementById('detail').className='empty';document.getElementById('detail').textContent='Select a reference to view its summary.'});const project=selected();document.getElementById('delete').disabled=!project;document.getElementById('add-ref').disabled=!project;document.getElementById('export').disabled=!project||!project.ref_ids.length;document.getElementById('refs-title').textContent=project?project.name:'References';const refs=document.getElementById('refs');refs.innerHTML=!project?'<div class="empty">Select a project.</div>':project.ref_ids.length?project.ref_ids.map(id=>{const cached=project.reference_cache?.[id];const marker=cached?.has_digest?'<span class="digest-marker" title="Cached LLM digest" aria-label="Cached LLM digest">D</span>':'';return '<button class="item '+(id===state.refId?'active':'')+'" data-ref="'+esc(id)+'"><strong>'+esc(cached?.title||id)+marker+'</strong><small>'+esc(id)+'</small></button>'}).join(''):'<div class="empty">This project has no references.</div>';refs.querySelectorAll('[data-ref]').forEach(b=>b.onclick=()=>showRef(b.dataset.ref))}
async function reload(preferred=state.projectId){const data=await api('/api/projects');state.projects=data.projects;state.projectId=state.projects.some(p=>p.project_id===preferred)?preferred:(state.projects[0]?.project_id||null);render()}
function showDetail(refId,entry){const detail=document.getElementById('detail');detail.className='detail';detail.innerHTML='<div class="detail-header"><div><h2>'+esc(entry.title)+'</h2><div class="ref">'+esc(refId)+'</div></div><button id="refresh-ref" class="icon" title="Refresh cached reference" aria-label="Refresh cached reference">↻</button></div><div class="detail-body">'+summaryHtml(entry.summary)+'</div><div class="detail-actions"><button id="remove-ref">Remove from project</button></div>';document.getElementById('refresh-ref').onclick=refreshRef;document.getElementById('remove-ref').onclick=removeRef}
async function showRef(id){const project=selected();if(!project)return;state.refId=id;render();const detail=document.getElementById('detail');detail.className='detail';detail.innerHTML='<p>Loading cached reference...</p>';try{const data=await api('/api/projects/'+project.project_id+'/refs/'+encodeURIComponent(id));await reload(project.project_id);state.refId=id;render();showDetail(id,data.reference)}catch(error){detail.className='empty';detail.textContent=error.message}}
async function refreshRef(){const project=selected();if(!project||!state.refId)return;const detail=document.getElementById('detail');detail.innerHTML='<p>Refreshing cached reference...</p>';try{const data=await api('/api/projects/'+project.project_id+'/refs/'+encodeURIComponent(state.refId)+'/refresh',{method:'POST'});await reload(project.project_id);showDetail(state.refId,data.reference)}catch(error){detail.className='empty';detail.textContent=error.message}}
async function addProject(){const input=document.getElementById('project-name');if(!input.value.trim())return;try{const data=await api('/api/projects',{method:'POST',body:JSON.stringify({name:input.value.trim()})});input.value='';await reload(data.project.project_id)}catch(e){alert(e.message)}}
async function deleteProject(){const p=selected();if(p&&confirm('Delete project '+p.name+'?')){await api('/api/projects/'+p.project_id,{method:'DELETE'});state.projectId=null;await reload()}}
async function addRef(){const p=selected(),input=document.getElementById('ref-id');if(p&&input.value.trim()){try{const data=await api('/api/projects/'+p.project_id+'/refs',{method:'POST',body:JSON.stringify({ref_id:input.value.trim()})});input.value='';await reload(p.project_id);showRef(data.ref_id)}catch(e){alert(e.message)}}}
async function removeRef(){const p=selected();if(p&&state.refId){await api('/api/projects/'+p.project_id+'/refs/'+encodeURIComponent(state.refId),{method:'DELETE'});state.refId=null;const detail=document.getElementById('detail');detail.className='empty';detail.textContent='Select a reference to view its summary.';await reload(p.project_id)}}
document.getElementById('add-project').onclick=addProject;document.getElementById('delete').onclick=deleteProject;document.getElementById('add-ref').onclick=addRef;document.getElementById('export').onclick=()=>{const p=selected();if(p)location.href='/api/projects/'+p.project_id+'/bib'};reload().catch(e=>alert(e.message));
const appMain=document.getElementById('app-main');
const toggleProjects=document.getElementById('toggle-projects');
toggleProjects.onclick=()=>{const hidden=appMain.classList.toggle('projects-hidden');toggleProjects.textContent=hidden?'Show projects':'Hide projects'};
const panelResizer=document.getElementById('panel-resizer');
panelResizer.onpointerdown=event=>{if(window.matchMedia('(max-width:900px)').matches)return;panelResizer.setPointerCapture(event.pointerId);panelResizer.classList.add('dragging');const resize=move=>{const bounds=appMain.getBoundingClientRect();const width=Math.max(320,Math.min(bounds.width-326,Math.round(bounds.right-move.clientX)));appMain.style.setProperty('--detail-width',width+'px')};resize(event);panelResizer.onpointermove=resize;panelResizer.onpointerup=()=>{panelResizer.onpointermove=null;panelResizer.classList.remove('dragging')}};
</script></body></html>`;

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) return usage();
  if (!args.projectsPath) throw new Error("--projects-path is required.");
  if (!Number.isInteger(args.port) || args.port < 1 || args.port > 65535) throw new Error("--port must be an integer from 1 to 65535.");
  const projectsPath = path.resolve(args.projectsPath);
  if (!fs.existsSync(projectsPath)) {
    const parent = path.dirname(projectsPath);
    if (!fs.existsSync(parent)) throw new Error(`Parent directory does not exist: ${parent}`);
    fs.mkdirSync(projectsPath);
  }
  if (!fs.statSync(projectsPath).isDirectory()) throw new Error(`Not a directory: ${projectsPath}`);
  migrateProjectReferenceCaches(projectsPath);
  const scriptDir = __dirname;
  const summaryScript = path.join(scriptDir, "report_ref_summary.sh");
  const bibScript = path.join(scriptDir, "write_bib_by_ref_ids.sh");

  const server = http.createServer(async (request, response) => {
    try {
      const url = new URL(request.url, "http://127.0.0.1");
      const parts = url.pathname.split("/").filter(Boolean).map(decodeURIComponent);
      if (request.method === "GET" && parts.length === 0) {
        response.writeHead(200, { "Content-Type": "text/html; charset=utf-8", "Cache-Control": "no-store" });
        return response.end(html);
      }
      if (request.method === "GET" && parts.join("/") === "api/projects") {
        const cache = readReferenceCache(projectsPath);
        return sendJson(response, { status: "ok", projects: readProjects(projectsPath).map((project) => projectForClient(project, cache)) });
      }
      if (request.method === "POST" && parts.join("/") === "api/projects") {
        const body = await readBody(request);
        const name = String(body.name || "").trim();
        if (!name) throw new Error("Project name must not be empty.");
        const id = projectIdFor(name, new Set(readProjects(projectsPath).map((p) => p.project_id)));
        return sendJson(response, { status: "ok", project: projectForClient(writeProject(projectsPath, { project_id: id, name, ref_ids: [] }), readReferenceCache(projectsPath)) }, 201);
      }
      if (parts[0] === "api" && parts[1] === "projects" && parts.length === 3) {
        const project = readProject(projectsPath, parts[2]);
        if (request.method === "DELETE") {
          fs.unlinkSync(projectPath(projectsPath, project.project_id));
          return sendJson(response, { status: "ok", project_id: project.project_id });
        }
      }
      if (parts[0] === "api" && parts[1] === "projects" && parts[3] === "refs") {
        const project = readProject(projectsPath, parts[2]);
        if (request.method === "POST" && parts.length === 4) {
          const body = await readBody(request);
          const refId = normalizeRefId(body.ref_id);
          if (!project.ref_ids.includes(refId)) project.ref_ids.push(refId);
          let cache = readReferenceCache(projectsPath);
          if (!cache.references[refId]) {
            cache.references[refId] = await hydrateReference(summaryScript, refId);
            cache = writeReferenceCache(projectsPath, cache);
          }
          return sendJson(response, { status: "ok", ref_id: refId, project: projectForClient(writeProject(projectsPath, project), cache) });
        } else if (request.method === "DELETE" && parts.length === 5) {
          const refId = normalizeRefId(parts[4]);
          project.ref_ids = project.ref_ids.filter((id) => id !== refId);
        } else if (request.method === "GET" && parts.length === 5) {
          const refId = normalizeRefId(parts[4]);
          if (!project.ref_ids.includes(refId)) throw new Error(`Reference is not in project: ${refId}`);
          let cache = readReferenceCache(projectsPath);
          if (!cache.references[refId]) {
            cache.references[refId] = await hydrateReference(summaryScript, refId);
            cache = writeReferenceCache(projectsPath, cache);
          }
          return sendJson(response, { status: "ok", ref_id: refId, reference: cache.references[refId] });
        } else if (request.method === "POST" && parts.length === 6 && parts[5] === "refresh") {
          const refId = normalizeRefId(parts[4]);
          if (!project.ref_ids.includes(refId)) throw new Error(`Reference is not in project: ${refId}`);
          const cache = readReferenceCache(projectsPath);
          cache.references[refId] = await hydrateReference(summaryScript, refId);
          writeReferenceCache(projectsPath, cache);
          return sendJson(response, { status: "ok", ref_id: refId, reference: cache.references[refId] });
        } else {
          throw new Error("Unsupported project reference operation.");
        }
        return sendJson(response, { status: "ok", project: projectForClient(writeProject(projectsPath, project), readReferenceCache(projectsPath)) });
      }
      if (request.method === "GET" && parts[0] === "api" && parts[1] === "projects" && parts[3] === "bib") {
        const project = readProject(projectsPath, parts[2]);
        const temporaryBib = path.join(os.tmpdir(), `litxr-${process.pid}-${Date.now()}.bib`);
        try {
          await run("/bin/zsh", [bibScript, "--output", temporaryBib, "--ref-ids", project.ref_ids.map(canonicalRefId).join(",")]);
          const bib = fs.readFileSync(temporaryBib);
          response.writeHead(200, { "Content-Type": "application/x-bibtex; charset=utf-8", "Content-Disposition": `attachment; filename="${project.project_id}.bib"`, "Cache-Control": "no-store" });
          return response.end(bib);
        } finally {
          if (fs.existsSync(temporaryBib)) fs.unlinkSync(temporaryBib);
        }
      }
      sendJson(response, { status: "error", error: "Not found" }, 404);
    } catch (error) {
      sendJson(response, { status: "error", error: error.message }, 400);
    }
  });

  server.listen(args.port, "127.0.0.1", () => {
    const url = `http://127.0.0.1:${args.port}/`;
    process.stdout.write(`projects_path=${projectsPath}\nurl=${url}\nPress Ctrl-C to stop.\n`);
    if (args.browser) {
      const opener = process.platform === "darwin" ? ["open", [url]] : process.platform === "win32" ? ["cmd", ["/c", "start", url]] : ["xdg-open", [url]];
      execFile(opener[0], opener[1], () => {});
    }
  });
}

main().catch((error) => {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
});

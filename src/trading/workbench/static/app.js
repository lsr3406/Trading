/* Local-only research display. Dynamic values are always inserted as text. */
const state = {overview:null, datasets:[], price:null, catalog:null, roadmap:null, jobs:[], alphaTab:"strategies"};
const names = {overview:"总览",data:"数据工厂",alpha:"因子与策略",research:"研究运行",execution:"模拟执行",roadmap:"数据路线"};
const jobNames = {doctor:"环境检查",collect:"数据采集","single-study":"单资产研究","catalog-study":"目录研究"};

function el(tag, attrs={}, ...children){
  const node=document.createElement(tag);
  for(const [key,value] of Object.entries(attrs)){
    if(key==="class") node.className=value;
    else if(key==="text") node.textContent=String(value ?? "");
    else node.setAttribute(key,String(value));
  }
  for(const child of children){if(child!=null) node.append(child instanceof Node ? child : document.createTextNode(String(child)));}
  return node;
}
function slot(id,...nodes){document.getElementById(id).replaceChildren(...nodes);}
function number(value,digits=0){return typeof value==="number" && Number.isFinite(value)?value.toLocaleString("zh-CN",{minimumFractionDigits:digits,maximumFractionDigits:digits}):"—";}
function percent(value,digits=2){return typeof value==="number"&&Number.isFinite(value)?`${number(value,digits)}%`:"—";}
function date(value){if(!value)return "—";const d=new Date(value);return Number.isNaN(d.getTime())?String(value):d.toLocaleDateString("zh-CN",{timeZone:"UTC",year:"numeric",month:"2-digit",day:"2-digit"});}
function badge(label,type=""){return el("span",{class:`badge ${type}`,text:label});}
function metric(label,value,sub,style=""){return el("div",{class:"metric-card"},el("div",{class:"metric-label"},el("span",{text:label}),el("span",{class:"metric-symbol",text:"◇"})),el("div",{class:`metric-value ${style}`,text:value}),el("div",{class:"metric-sub",text:sub}));}
function empty(message){return el("div",{class:"empty",text:message});}
function table(headers,rows){
  if(!rows.length)return empty("暂无可展示的记录。运行对应研究流程后会在这里出现。");
  const head=el("thead",{},el("tr",{},...headers.map(h=>el("th",{text:h}))));
  const body=el("tbody",{},...rows.map(row=>el("tr",{},...row.map(cell=>el("td",{},cell instanceof Node?cell:String(cell ?? "—"))))));
  return el("table",{class:"data-table"},head,body);
}
function summaryRow(label,value){return el("div",{class:"summary-row"},el("span",{text:label}),el("strong",{text:value}));}
function finding(title,value,note){return el("div",{class:"finding"},el("div",{class:"finding-title",text:title}),el("div",{class:"finding-value",text:value}),el("div",{class:"finding-note",text:note}));}
function toast(message,error=false){const box=document.getElementById("toast");box.textContent=message;box.className=error?"show error":"show";clearTimeout(toast.timer);toast.timer=setTimeout(()=>{box.className="";},4400);}
async function json(url,options){const response=await fetch(url,options);if(!response.ok){let detail=`HTTP ${response.status}`;try{const body=await response.json();detail=typeof body.detail==="string"?body.detail:detail;}catch{}throw new Error(detail);}return response.json();}

function renderOverview(){
  const o=state.overview||{}, p=o.primary_dataset||{}, q=p.quality||{}, c=o.catalog_report||{}, s=o.single_report||{};
  slot("overview-metrics",metric("本地数据版本",number(o.dataset_count),p.id||"尚无已处理数据"),metric("覆盖 K 线",number(p.rows),p.symbol?`${p.symbol} · ${p.timeframe||""}`:"等待数据采集"),metric("因子候选",number(c.factor_count),c.generated_at_utc?`报告更新 ${date(c.generated_at_utc)}`:"等待目录研究"),metric("策略入选",c.selected_strategy?"1":"0",c.selected_strategy?"开发阶段有候选":"当前未通过完整筛选",c.selected_strategy?"accent":"warn"));
  const quality=q.ok===true?"通过":q.ok===false?"未通过":"未检查";
  slot("overview-findings",finding("数据质量",quality,q.metrics?`缺失 ${number(q.metrics.missing_bars)} · 重复 ${number(q.metrics.duplicates)} · 无效 ${number(q.metrics.invalid_bars)}`:"运行采集后生成质量报告"),finding("目录研究筛选",c.selected_strategy?c.selected_strategy.name||"有候选":"无入选策略",c.strategy_count?`${c.strategy_count} 个策略候选，采用多重比较校正。`:`运行目录研究后查看结果。`),finding("单资产最终保留集",s.final_holdout?percent(s.final_holdout.return_pct):"尚无报告",s.final_holdout?`BTC 4h 动量研究 · ${s.final_holdout.bars} 根 K 线；历史表现不代表未来收益。`:"运行单资产研究后查看证据。"));
  renderChart();
}
function renderChart(){
  const data=state.price||{}, points=(data.points||[]).filter(p=>typeof p.close==="number"&&Number.isFinite(p.close));
  document.getElementById("chart-dataset").textContent=data.dataset||"暂无数据";
  document.getElementById("chart-start").textContent=points.length?date(points[0].time):"—";
  document.getElementById("chart-end").textContent=points.length?date(points[points.length-1].time):"—";
  if(points.length<2){slot("price-chart",empty("暂无价格序列。先运行数据采集。"));return;}
  const min=Math.min(...points.map(p=>p.close)),max=Math.max(...points.map(p=>p.close));
  const spread=Math.max(max-min,Math.abs(max)*0.03,1),low=min-spread*0.12,high=max+spread*0.12;
  const coords=points.map((p,i)=>[45+i*850/(points.length-1),205-(p.close-low)*170/(high-low)]);
  const line=coords.map(([x,y],i)=>`${i?"L":"M"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ");
  const svg=document.createElementNS("http://www.w3.org/2000/svg","svg");svg.setAttribute("viewBox","0 0 920 225");svg.setAttribute("preserveAspectRatio","none");svg.setAttribute("role","img");svg.setAttribute("aria-label","本地资产收盘价轨迹");
  function path(d,stroke,fill="none",width="1"){const p=document.createElementNS("http://www.w3.org/2000/svg","path");p.setAttribute("d",d);p.setAttribute("stroke",stroke);p.setAttribute("fill",fill);p.setAttribute("stroke-width",width);return p;}
  for(let i=0;i<4;i++){const y=35+i*55;svg.append(path(`M45 ${y} L895 ${y}`,"#27404b"));}
  for(const [y,value] of [[38,max],[205,min]]){const label=document.createElementNS("http://www.w3.org/2000/svg","text");label.setAttribute("x","2");label.setAttribute("y",String(y));label.setAttribute("fill","#7897a2");label.setAttribute("font-size","10");label.textContent=number(value,0);svg.append(label);}
  const defs=document.createElementNS("http://www.w3.org/2000/svg","defs"),grad=document.createElementNS("http://www.w3.org/2000/svg","linearGradient");grad.setAttribute("id","price-fill");grad.setAttribute("x1","0");grad.setAttribute("y1","0");grad.setAttribute("x2","0");grad.setAttribute("y2","1");
  for(const [offset,opacity] of [["0%","0.28"],["100%","0"]]){const stop=document.createElementNS("http://www.w3.org/2000/svg","stop");stop.setAttribute("offset",offset);stop.setAttribute("stop-color","#43d5aa");stop.setAttribute("stop-opacity",opacity);grad.append(stop);}defs.append(grad);svg.append(defs);
  svg.append(path(`${line} L895 210 L45 210 Z`,"none","url(#price-fill)"),path(line,"#50d6af","none","2.1"));
  slot("price-chart",svg);
}
function renderData(){
  const items=state.datasets, good=items.filter(d=>d.quality?.ok===true), bad=items.filter(d=>d.quality?.ok===false);
  slot("data-metrics",metric("数据版本",number(items.length),"版本化 Parquet"),metric("质量通过",number(good.length),"已生成质量报告"),metric("质量问题",number(bad.length),"需复核的版本",bad.length?"warn":""),metric("总记录数",number(items.reduce((n,d)=>n+(d.rows||0),0)),"所有已处理数据行"));
  const rows=items.map(d=>[el("strong",{text:d.id}),d.symbol||"—",d.timeframe||"—",number(d.rows),`${date(d.start)} → ${date(d.end)}`,d.quality?badge(d.quality.ok?"通过":"未通过",d.quality.ok?"":"bad"):badge("未检查","muted")]);
  slot("dataset-table",table(["数据版本","资产","周期","行数","时间范围 (UTC)","质量"],rows));
}
function renderAlpha(){
  const c=state.catalog||{},r=c.report||{},tab=state.alphaTab;
  slot("alpha-metrics",metric("因子定义",number(c.factors?.length),"按窗口与家族预声明"),metric("策略假设",number(c.strategies?.length),"每个参数组合计一次试验"),metric("显著因子",number(r.factor_significant_count),"仅表示统计筛选，不是收益"),metric("入选策略",r.selected_strategy?"1":"0","开发期多重检验结果",r.selected_strategy?"accent":"warn"));
  document.getElementById("alpha-correction").textContent=`校正方法：${c.correction||"—"} · α=${c.significance_level??"—"}`;
  document.querySelectorAll("[data-alpha-tab]").forEach(btn=>btn.classList.toggle("active",btn.dataset.alphaTab===tab));
  if(tab==="strategies"){
    document.getElementById("alpha-table-title").textContent="策略试验";
    const rows=[...(r.strategy_trials||[])].sort((a,b)=>(a.adjusted_p??1)-(b.adjusted_p??1)).map(s=>{
      const selected=s.adjusted_p<=c.significance_level && s.mean_excess_bar_return>0;
      const label=selected?"通过筛选":s.adjusted_p<=c.significance_level?"显著但负超额":"未通过";
      return [el("strong",{text:s.name}),s.kind,percent(s.mean_fold_return_pct),percent(s.mean_fold_buy_hold_return_pct),number(s.orders),number(s.adjusted_p,4),badge(label,selected?"":s.adjusted_p<=c.significance_level?"warn":"muted")];
    });
    slot("alpha-table",table(["策略","类型","平均样本外收益","同额持有","订单","校正 P 值","筛选"],rows));
  }else if(tab==="factors"){
    document.getElementById("alpha-table-title").textContent="因子试验";
    const rows=[...(r.factor_trials||[])].sort((a,b)=>(a.adjusted_p??1)-(b.adjusted_p??1)).map(f=>[el("strong",{text:f.name}),number(f.mean_block_ic,4),number(f.ic_ir,3),number(f.oos_ic_blocks),number(f.adjusted_p,4),badge(f.adjusted_p<=c.significance_level?"统计显著":"未通过",f.adjusted_p<=c.significance_level?"":"muted")]);
    slot("alpha-table",table(["因子","分块 IC","IC IR","样本外分块","校正 P 值","结果"],rows));
  }else{
    document.getElementById("alpha-table-title").textContent="预声明候选";
    const strategies=(c.strategies||[]).map(s=>[el("strong",{text:s.name}),s.kind,JSON.stringify(s.parameters||{})]);
    slot("alpha-table",el("div",{},el("p",{class:"panel-hint",text:`因子名称：${(c.factors||[]).join(" · ")}`}),table(["策略","类型","参数"],strategies)));
  }
}
function renderResearch(){
  const jobs=state.jobs||[];
  slot("job-list",...(jobs.length?jobs.map(j=>{const status=j.status==="completed"?badge("完成"):j.status==="failed"?badge("失败","bad"):badge("运行中","warn");return el("div",{class:"job"},el("div",{class:"job-head"},el("strong",{text:jobNames[j.command]||j.command}),status),el("div",{class:"job-meta",text:`${date(j.started_at_utc)} · ${j.status}`}),j.output?el("pre",{text:j.output}):null);}):[empty("本次工作台尚未运行任务。已有研究报告仍可在其他页面查看。")]));
  const o=state.overview||{},s=o.single_report||{},c=o.catalog_report||{};
  slot("research-summary",summaryRow("单资产研究",s.generated_at_utc?date(s.generated_at_utc):"未运行"),summaryRow("最终保留集收益",s.final_holdout?percent(s.final_holdout.return_pct):"—"),summaryRow("目录研究",c.generated_at_utc?date(c.generated_at_utc):"未运行"),summaryRow("候选策略",c.strategy_count??"—"),summaryRow("目录入选",c.selected_strategy?.name||"无"),summaryRow("最终保留集",c.holdout_status?"目录未重新评估":"—"));
  const busy=jobs.some(j=>j.status==="running"||j.status==="queued");document.querySelectorAll("[data-command]").forEach(btn=>{btn.disabled=busy;});
}
function renderExecution(){
  const e=state.overview?.execution||{};
  slot("execution-metrics",metric("执行模式","禁用","研究与执行严格分离"),metric("模拟引擎",e.paper_engine==="configured"?"已配置":"待校准","NautilusTrader 离线模拟",e.paper_engine==="configured"?"accent":"warn"),metric("实盘连接","不可用","无下单接口"),metric("风控参数",number((e.missing_paper_fields||[]).length),"待填写的模拟参数"));
  const missing=e.missing_paper_fields||[];slot("paper-fields",...(missing.length?missing.map(name=>el("span",{class:"tag",text:name})):[badge("已填写主要参数；仍需独立验证")]));
}
function renderRoadmap(){
  const plan=state.roadmap||{},sources=plan.sources||[];
  if(!sources.length){slot("roadmap-list",empty("暂无路线图。"));return;}
  const cards=sources.map((s,i)=>el("article",{class:"roadmap-card"},
    el("div",{class:"roadmap-head"},
      el("div",{class:"roadmap-number",text:String(i+1).padStart(2,"0")}),
      el("div",{},el("h2",{text:s.name}),el("div",{class:"category",text:s.category}))),
    el("p",{},badge(s.status,"warn")),
    el("p",{text:s.evidence}),
    el("p",{class:"next",text:`下一步：${s.next_step}`}),
    el("p",{text:s.needs_user?"后续需要用户提供只读 API 凭据。":"当前无需账户登录。"}),
    el("a",{href:s.url,target:"_blank",rel:"noopener noreferrer",text:"官方来源 ↗"})));
  slot("roadmap-list",...cards);
}
function render(){renderOverview();renderData();renderAlpha();renderResearch();renderExecution();renderRoadmap();document.getElementById("updated-at").textContent=`更新于 ${new Date().toLocaleTimeString("zh-CN")}`;}
async function loadAll(){
  try{const [overview,datasets,price,catalog,roadmap,jobs]=await Promise.all([json("/api/overview"),json("/api/datasets"),json("/api/price"),json("/api/catalog"),json("/api/roadmap"),json("/api/jobs")]);Object.assign(state,{overview,datasets,price,catalog,roadmap,jobs});render();}
  catch(error){toast(`加载失败：${error.message}`,true);}
}
function view(name){document.querySelectorAll(".view").forEach(e=>e.classList.toggle("active",e.id===`view-${name}`));document.querySelectorAll(".nav-item").forEach(e=>e.classList.toggle("active",e.dataset.view===name));document.getElementById("breadcrumb-current").textContent=names[name];window.scrollTo(0,0);}
async function startJob(command){
  try{await json("/api/jobs",{method:"POST",headers:{"Content-Type":"application/json","X-Workbench-Token":document.querySelector('meta[name="workbench-token"]').content},body:JSON.stringify({command})});toast(`${jobNames[command]} 已启动`);view("research");await loadAll();}
  catch(error){toast(`启动失败：${error.message}`,true);}
}
document.addEventListener("click",event=>{const target=event.target.closest("button");if(!target)return;if(target.dataset.view)view(target.dataset.view);if(target.dataset.viewLink)view(target.dataset.viewLink);if(target.dataset.refresh!==undefined)loadAll();if(target.dataset.alphaTab){state.alphaTab=target.dataset.alphaTab;renderAlpha();}if(target.dataset.command)startJob(target.dataset.command);});
setInterval(async()=>{try{const jobs=await json("/api/jobs");const previous=state.jobs.some(j=>j.status==="running"||j.status==="queued");state.jobs=jobs;renderResearch();if(previous&&!jobs.some(j=>j.status==="running"||j.status==="queued")){await loadAll();toast("研究任务已结束，报告已刷新");}}catch{}},3500);
loadAll();

// Private audit presentation only. Does not fetch data or change reporting/runtime.
// Run a copy in an ignored directory with the Codex bundled node_modules junction.
import fs from 'node:fs/promises';
import path from 'node:path';
import { Workbook, SpreadsheetFile } from '@oai/artifact-tool';

const [inputDir, outputDir] = process.argv.slice(2);
if (!inputDir || !outputDir) throw new Error('Usage: builder inputDir outputDir');
const read = async name => JSON.parse(await fs.readFile(path.join(inputDir, name), 'utf8'));
const [source, costs, fixed, examples, meta, google] = await Promise.all([
  read('manual_daily_orders_20261007.json'), read('manual_product_cost_audit_20261007.json'),
  read('fixed_cost_fresh_source_audit_20261007.json'), read('manual_product_cost_examples_20261007.json'),
  read('aggregates/independent_meta_country_daily_20261007.json'),
  read('aggregates/independent_google_country_daily_20261007.json'),
]);
let auditStatus = null;
try { auditStatus = await read('manual_audit_status_20261007.json'); }
catch (error) { if (error.code !== 'ENOENT') throw error; }
if (!source.complete_boundary || !fixed.complete_source_boundary) throw new Error('Incomplete source');
const sorted = [...source.orders].sort((a,b) => a.date.localeCompare(b.date) || a.order_ref.localeCompare(b.order_ref));
const byRef = new Map(costs.orders.map(o => [o.order_ref_private, o]));
if (byRef.size !== costs.orders.length || new Set(sorted.map(o=>o.order_ref)).size !== sorted.length) throw new Error('Duplicate order');
const wb = Workbook.create();
const summary = wb.worksheets.add('Kontrola');
const exampleSheet = wb.worksheets.add('Príklady');
const orders = wb.worksheets.add('Objednávky');
const products = wb.worksheets.add('Produkty');
const money = '#,##0.00;(#,##0.00);0.00';
const dayRow = new Map();
const orderRow = new Map();
const itemRanges = new Map();
const serial = d => new Date(d+'T00:00:00Z');
function style(sheet, range, title) {
  sheet.showGridLines = false;
  sheet.getRange(range).format.font = {name:'Arial',size:10,color:'#202B37'};
  sheet.getRange(range).format.rowHeight = 21;
  sheet.getRange(range).format.verticalAlignment = 'center';
  sheet.getRange(range).format.columnWidth = 15;
  sheet.getRange('A2').values = [[title]];
  sheet.getRange('A2').format.font = {name:'Arial',size:16,bold:true,color:'#253E5B'};
  sheet.getRange(range.split(':')[0]+':'+range.split(':')[1].replace(/\d+$/,'3')).format.borders = {bottom:{style:'thin',color:'#CED5DC'}};
}
function header(sheet, range, labels) {
  sheet.getRange(range).values = [labels];
  sheet.getRange(range).format = {fill:'#253E5B',font:{name:'Arial',size:10,bold:true,color:'#FFFFFF'},wrapText:true,horizontalAlignment:'center',rowHeight:40,verticalAlignment:'center'};
}
function warn(sheet, range, formula) {
  sheet.getRange(range).conditionalFormats.addCustom(formula,{fill:'#FCE7E7',font:{color:'#A32121',bold:true}});
}
const basis = b => ({configured_reference_cost:'Konfigurovaná cena',missing_cost_margin_estimate:'Odhad nákladu',explicit_margin_policy:'Model: maržová politika',authoritative_margin_policy:'Model: maržová politika',zero_cost_service_override:'Služba: nulový náklad',zero_cost_service_policy:'Služba: nulový náklad'})[b] || b;

style(products,'A1:S'+(costs.item_rows+7),'Položky objednávok');
products.getRange('A4').values = [['Zdroj: čerstvé BiznisWeb API + nákladová konfigurácia zo zdrojového commitu v podkladoch. Bez mien a adries.']];
header(products,'A6:S6',['Objednávka','Krajina','Produkt','Ks','Mena','Cena/ks bez DPH','Riadok bez DPH','Riadok s DPH','DPH %','EUR / mena','EUR pred zľavou','Zľava netto EUR','EUR po zľave','Náklad reportu EUR','Marža produktu EUR','Základ nákladu','Referenčný náklad/ks','SKU','Rozdiel nákladu ručne']);
let pr=7;
for (const order of sorted) {
  const co=byRef.get(order.order_ref); if (!co) throw new Error('Missing cost order');
  const first=pr;
  for (const item of co.items) {
    const applied=item.applied_cost_evidence;
    const netDiscount = item.production.item_order_discount_without_tax ?? Math.round(Number(item.net_eur_before_order_discount)*Number(item.order_discount_pct))/100;
    products.getRange(`A${pr}:S${pr}`).values = [[order.order_ref,order.country,item.label,Number(item.quantity),item.currency,Number(item.api_unit_price_original),Number(item.api_line_net_original),Number(item.api_line_gross_original),Number(item.tax_rate_pct)/100,Number(item.fixed_fx_to_eur),Number(item.net_eur_before_order_discount),netDiscount,null,Number(item.production.total_expense),null,basis(applied.basis)+(applied.margin_pct?' ('+applied.margin_pct+' %)':''),item.reference_unit_cost_eur===null?null:Number(item.reference_unit_cost_eur),item.sku,Number(item.production.total_expense)-Number(item.cost_eur)]];
    products.getRange(`M${pr}`).formulas=[[`=ROUND(K${pr}-L${pr},2)`]];
    products.getRange(`O${pr}`).formulas=[[`=M${pr}-N${pr}`]];
    pr++;
  }
  itemRanges.set(order.order_ref,[first,pr-1]);
}
products.getRange(`F7:S${pr-1}`).setNumberFormat(money);
products.getRange(`I7:I${pr-1}`).setNumberFormat('0.0%');
products.getRange(`J7:J${pr-1}`).setNumberFormat('0.0000');
products.getRange(`R7:R${pr-1}`).setNumberFormat('@');
products.getRange(`R6:R${pr}`).format.columnWidth=22;
products.getRange(`A6:A${pr}`).format.columnWidth=17;
products.getRange(`B6:B${pr}`).format.columnWidth=8;
products.getRange(`C6:C${pr}`).format.columnWidth=65;
products.getRange(`P6:P${pr}`).format.columnWidth=28;
products.freezePanes.freezeRows(6); products.freezePanes.freezeColumns(3);
products.tables.add(`A6:S${pr-1}`,true,'AuditProducts');
warn(products,`S7:S${pr-1}`,'ABS(S7)>0.02');

style(orders,'A1:Z'+(sorted.length+7),'Objednávky a rozpočítané náklady');
orders.getRange('A4').values=[['Reklama = skutočná útrata krajiny a dňa / uznané objednávky krajiny a dňa. Ide o priemer, nie priradenú konverziu.']];
header(orders,'A6:T6',['Objednávka','Dátum','Krajina','Stav','Uznaná 1/0','Dôvod','Tržba pred zľavou EUR','Zľava netto EUR','Tržba po zľave EUR','Náklad produktov EUR','Produktová marža EUR','Balné EUR','Doprava netto EUR','Fix / objednávka EUR','Reklama / objednávka EUR','Výsledok po alokácii EUR','Počet odhadov 35 %','Mena','Suma objednávky v mene','Platba ID']);
header(orders,'U6:Z6',['Tovar s DPH v mene','Doprava s DPH v mene','Platba s DPH v mene','Zaokrúhlenie v mene','Zľava s DPH v mene','Rozdiel súčtu v mene']);
let r=7;
for (const o of sorted) {
  const co=byRef.get(o.order_ref), [start,end]=itemRanges.get(o.order_ref);
  const fd=fixed.days.find(d=>d.date===o.date); if (!fd) throw new Error('Missing fixed-cost day');
  const count=fd.order_counts[o.country]||0;
  const m=meta.country_daily.filter(x=>x.date_start===o.date&&x.country===o.country).reduce((s,x)=>s+Number(x.spend),0);
  const g=google.rows.filter(x=>x.date===o.date&&x.country===o.country).reduce((s,x)=>s+Number(x.spend),0);
  if (o.included && !count) throw new Error('Missing allocation denominator');
  orders.getRange(`A${r}:T${r}`).values=[[o.order_ref,serial(o.date),o.country,o.canonical_status,o.included?1:0,o.inclusion_reason,o.net_goods_eur,o.net_goods_eur-o.discount_adjusted_net_goods_eur,null,null,null,o.included?Number(co.model_packaging_eur):0,o.included?Number(co.model_shipping_net_cost_eur):0,o.included?Number(fd.fixed_global_eur)/fd.eligible_orders:0,o.included?(m+g)/count:0,null,co.items.filter(i=>i.applied_cost_evidence.basis==='missing_cost_margin_estimate').length,o.currency,Number(o.provider_order_total_original),o.payment_id]];
  orders.getRange(`I${r}:K${r}`).formulas=[[`=G${r}-H${r}`,`=SUM('Produkty'!N${start}:N${end})`,`=I${r}-J${r}`]];
  orders.getRange(`P${r}`).formulas=[[`=IF(E${r}=1,K${r}-SUM(L${r}:O${r}),"nezahrnutá")`]];
  const fee = kind => o.price_elements.filter(e=>e.type===kind).reduce((sum,e)=>sum+Number(e.gross_from_single_item_vat_original),0);
  const grossDiscount=-(o.gross_goods_eur-o.discount_adjusted_gross_goods_eur)/Number(co.fixed_fx_to_eur);
  orders.getRange(`U${r}:Z${r}`).values=[[Number(o.gross_goods_original),fee('shipping'),fee('payment'),fee('autoround'),grossDiscount,null]];
  orders.getRange(`Z${r}`).formulas=[[`=ROUND(SUM(U${r}:Y${r})-S${r},2)`]];
  orderRow.set(o.order_ref,r++);
}
orders.getRange(`B7:B${r-1}`).setNumberFormat('yyyy-mm-dd');
orders.getRange(`G7:P${r-1}`).setNumberFormat(money);
orders.getRange(`S7:S${r-1}`).setNumberFormat(money);
orders.getRange(`U7:Z${r-1}`).setNumberFormat(money);
orders.getRange(`A6:A${r}`).format.columnWidth=17;
orders.getRange(`D6:D${r}`).format.columnWidth=26;
orders.getRange(`F6:F${r}`).format.columnWidth=31;
orders.freezePanes.freezeRows(6);orders.freezePanes.freezeColumns(3);
orders.tables.add(`A6:Z${r-1}`,true,'AuditOrders');
warn(orders,`H7:H${r-1}`,'ABS(H7)>0.02');
warn(orders,`Z7:Z${r-1}`,'ABS(Z7)>0.02');

style(summary,'A1:K55','VEVO: ručná kontrola reportingu');summary.tabColor='#253E5B';
summary.getRange('A4').values=[[auditStatus?.status_text || 'Stav: predbežná kontrola. Potvrdenie opráv a publikovania nie je súčasťou týchto vstupov.']];
summary.getRange('A4').format.font.color=auditStatus?.published_verified?'#202B37':'#A32121';
header(summary,'A6:J6',['Dátum','Všetky obj.','Uznané','Vyradené','Tržba pred zľavou EUR','Tržba po zľave EUR','Rozdiel EUR','Fix celkom EUR','Rozdelený fix EUR','Rozdiel fixov EUR']);
for(let i=0;i<fixed.days.length;i++) {
  const d=fixed.days[i],rr=7+i;dayRow.set(d.date,rr);
  summary.getRange(`A${rr}:J${rr}`).values=[[serial(d.date),null,null,null,null,null,null,Number(d.fixed_global_eur),null,null]];
  const dateRange=`'Objednávky'!$B$7:$B$${r-1}`, incRange=`'Objednávky'!$E$7:$E$${r-1}`;
  summary.getRange(`B${rr}:G${rr}`).formulas=[[`=COUNTIFS(${dateRange},A${rr})`,`=SUMIFS(${incRange},${dateRange},A${rr})`,`=B${rr}-C${rr}`,`=SUMIFS('Objednávky'!$G$7:$G$${r-1},${dateRange},A${rr},${incRange},1)`,`=SUMIFS('Objednávky'!$I$7:$I$${r-1},${dateRange},A${rr},${incRange},1)`,`=E${rr}-F${rr}`]];
  summary.getRange(`I${rr}:J${rr}`).formulas=[[`=SUMIFS('Objednávky'!$N$7:$N$${r-1},${dateRange},A${rr})`,`=ROUND(I${rr}-H${rr},2)`]];
}
summary.getRange('A7:A12').setNumberFormat('yyyy-mm-dd');summary.getRange('E7:J14').setNumberFormat(money);
summary.getRange('A14').values=[['Spolu']];
for (const col of 'BCDEFGHIJ') summary.getRange(`${col}14`).formulas=[[`=SUM(${col}7:${col}12)`]];
summary.getRange('A14:J14').format.font.bold=true;
warn(summary,'G7:G14','ABS(G7)>0.02');warn(summary,'J7:J14','ABS(J7)>0.02');
header(summary,'A17:G17',['Dátum','Krajina','Uznané obj.','Tržba po zľave EUR','Produktové náklady EUR','Fix krajiny EUR','Fix / obj. EUR']);
for(let i=0;i<source.daily_country_summary.length;i++) {
  const x=source.daily_country_summary[i],rr=18+i;
  summary.getRange(`A${rr}:G${rr}`).values=[[serial(x.date),x.country,null,null,null,null,null]];
  const dr=`'Objednávky'!$B$7:$B$${r-1}`,cr=`'Objednávky'!$C$7:$C$${r-1}`,ir=`'Objednávky'!$E$7:$E$${r-1}`;
  summary.getRange(`C${rr}:G${rr}`).formulas=[[`=COUNTIFS(${dr},A${rr},${cr},B${rr},${ir},1)`,...['I','J','N'].map(c=>`=SUMIFS('Objednávky'!$${c}$7:$${c}$${r-1},${dr},A${rr},${cr},B${rr},${ir},1)`),`=IF(C${rr}=0,"n.a.",F${rr}/C${rr})`]];
}
summary.getRange('A18:A35').setNumberFormat('yyyy-mm-dd');summary.getRange('D18:G35').setNumberFormat(money);
const notes=[
'Zdroj: BiznisWeb API, kompletné vybrané dni; stav pri získaní dát je v priložených súkromných podkladoch.',
'Tržby = tovar bez DPH, bez samostatných poplatkov za dopravu a platbu. Meny sa prevádzajú pevným kurzom reportingu.',
'Náklady produktov sú z konfigurácie. Odhad marže 35 % a explicitná maržová politika nie sú dodávateľské faktúry.',
'Sprepitné a poistenie majú podľa pokynu majiteľa nulový náklad. Historická referenčná mapa sa zobrazuje samostatne.',
'Balné a netto doprava sú modelové sadzby. Reklama na objednávku je priemer krajiny a dňa, nie presná atribúcia.',
'Dni bez uznaných objednávok nesú fix v celkovom reporte bez alokácie krajine. Dobropisové fulfillment náklady sú globálne.',
'Centové odchýlky zaokrúhľovania sú oddelené od chyby zľavy. Kontrola nepotvrdzuje bezchybnosť celého historického obdobia.',
'Nezahrnuté objednávky majú položky na kontrolu, ale nevstupujú do súhrnných tržieb ani rozdelenia fixov.',
];
notes.forEach((n,i)=>summary.getRange(`A${38+i}`).values=[[n]]);
summary.getRange('A6:A35').format.columnWidth=16;summary.getRange('E6:J35').format.columnWidth=19;

style(exampleSheet,'A1:H'+(examples.examples.length*17+8),'Príklady objednávok na kontrolu');
exampleSheet.getRange('A4').values=[['Marža produktu = tržba bez DPH po zľave − produktový náklad. Výsledok objednávky zahŕňa modelové alokácie.']];
exampleSheet.getRange('A5').values=[['Hodnoty sú pre kontrolu konfigurácie; dodávateľské a kuriérske faktúry neboli súčasťou zdrojov.']];
let er=7;
for(const ex of examples.examples) {
  const or=orderRow.get(ex.order_ref_private),[start,end]=itemRanges.get(ex.order_ref_private);
  exampleSheet.getRange(`A${er}`).values=[[`${ex.country} ${ex.order_ref_private} · ${ex.date}`]];
  exampleSheet.getRange(`A${er}`).format.font.bold=true;
  er++;
  header(exampleSheet,`A${er}:E${er}`,['Produkt','Ks','Tržba po zľave EUR','Náklad EUR','Marža EUR']);er++;
  for(let ir=start;ir<=end;ir++,er++) exampleSheet.getRange(`A${er}:E${er}`).formulas=[[`='Produkty'!C${ir}`,`='Produkty'!D${ir}`,`='Produkty'!M${ir}`,`='Produkty'!N${ir}`,`='Produkty'!O${ir}`]];
  for(const [label,col] of [['Tržba bez DPH po zľave','I'],['Produktové náklady','J'],['Produktová marža','K'],['Balné','L'],['Netto doprava','M'],['Podiel fixov','N'],['Priemer reklamy krajiny/dňa','O'],['Výsledok po alokácii','P']]) {
    exampleSheet.getRange(`A${er}`).values=[[label]];exampleSheet.getRange(`C${er}`).formulas=[[`='Objednávky'!${col}${or}`]];er++;
  }
  er++;
}
exampleSheet.getRange(`A6:A${er}`).format.columnWidth=76;
exampleSheet.getRange(`B6:B${er}`).format.columnWidth=8;
exampleSheet.getRange(`C6:E${er}`).format.columnWidth=19;
exampleSheet.getRange(`C6:E${er}`).setNumberFormat(money);
wb.recalculate();
await fs.mkdir(outputDir,{recursive:true});
const overview=await wb.inspect({kind:'table',range:'Kontrola!A6:J14',include:'values,formulas',tableMaxRows:9,tableMaxCols:10,maxChars:10000});
await fs.writeFile(path.join(outputDir,'verification.jsonl'),overview.ndjson);
const errs=await wb.inspect({kind:'match',searchTerm:'#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!',options:{useRegex:true,maxResults:100},summary:'Formula error scan'});
await fs.writeFile(path.join(outputDir,'formula-check.jsonl'),errs.ndjson);
const totals=summary.getRange('B14:J14').values[0];
if(totals[0]!==sorted.length || totals[1]!==sorted.filter(o=>o.included).length || Math.abs(totals[8])>0.0001) throw new Error('Reconciliation failed');
const expectedRevenue=sorted.filter(o=>o.included).reduce((sum,o)=>sum+o.discount_adjusted_net_goods_eur,0);
if(Math.abs(totals[4]-expectedRevenue)>0.0001) throw new Error('Source revenue reconciliation failed');
if(orders.getRange(`Z7:Z${r-1}`).values.some(row=>!Number.isFinite(row[0]) || Math.abs(row[0])>0.02)) throw new Error('Native order total reconciliation failed');
// Verify a representative source edit recalculates a dependent order output, then restore it.
const old=orders.getRange('H7').values[0][0],before=orders.getRange('I7').values[0][0];
orders.getRange('H7').values=[[old+1]];
if(Math.abs(orders.getRange('I7').values[0][0]-(before-1))>0.0001) throw new Error('Recalculation failed');
orders.getRange('H7').values=[[old]];wb.recalculate();
for(const [sheetName,range,name] of [['Kontrola','A1:J14','kontrola'],['Príklady','A7:E20','priklady'],['Objednávky','A6:J12','objednavky'],['Objednávky','S6:Z12','objednavky-sucty'],['Produkty','A6:I12','produkty'],['Produkty','J6:S12','produkty-naklady']]) {
  const preview=await wb.render({sheetName,range,scale:1.4,format:'png'});
  await fs.writeFile(path.join(outputDir,name+'.png'),new Uint8Array(await preview.arrayBuffer()));
}
const xlsx=await SpreadsheetFile.exportXlsx(wb);
await xlsx.save(path.join(outputDir,'VEVO_rucna_kontrola_2026-10-07.xlsx'));
console.log(JSON.stringify({orders:sorted.length,items:costs.item_rows,sheets:4,output:path.join(outputDir,'VEVO_rucna_kontrola_2026-10-07.xlsx')}));

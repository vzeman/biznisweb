#!/usr/bin/env node
/**
 * Build an aggregate-only yearly comparison from a generated report HTML.
 * Requires @oai/artifact-tool from the bundled workspace dependencies.
 * Usage: node build_vevo_year_comparison.mjs --input report.html --output result.xlsx
 *   [--year YYYY] [--requested-through YYYY-MM-DD] [--dependencies node_modules]
 *   [--qa-dir directory]
 * The HTML is treated strictly as data. Embedded scripts are never executed.
 */
import fs from 'node:fs/promises';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { createHash } from 'node:crypto';

const args = {};
for (let i = 2; i < process.argv.length; i += 2) {
  const key = process.argv[i];
  if (!/^--[a-z-]+$/.test(key) || !process.argv[i + 1]) throw new Error('Expected --name value arguments');
  args[key.slice(2)] = process.argv[i + 1];
}
if (!args.input || !args.output) throw new Error('Required arguments: --input report.html --output result.xlsx');
const allowed = new Set(['input', 'output', 'year', 'requested-through', 'dependencies', 'qa-dir']);
for (const key of Object.keys(args)) if (!allowed.has(key)) throw new Error(`Unknown option: ${key}`);
if (path.extname(args.output).toLowerCase() !== '.xlsx') throw new Error('Output must end in .xlsx');

const html = await fs.readFile(args.input, 'utf8');
const generatedAt = html.match(/<small>\s*<span[^>]*>Generated<\/span>[\s\S]*?<\/small>\s*<strong>(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})<\/strong>/)?.[1] ?? 'v zdroji neuvedené';
const sourceHash = createHash('sha256').update(html).digest('hex');
const blocks = [...html.matchAll(/<script\b(?=[^>]*\bid=["']report-dashboard-json["'])[^>]*>([\s\S]*?)<\/script>/gi)];
if (blocks.length !== 1) throw new Error('Expected exactly one report-dashboard-json script');
const report = JSON.parse(blocks[0][1]);
const series = report.series;
const sourceFields = ['revenue', 'orders', 'product_cost', 'packaging', 'shipping', 'fb_ads', 'google_ads', 'total_ads', 'fixed', 'profit_without_fixed', 'profit_with_fixed'];
if (!Array.isArray(series?.dates) || !series.dates.length) throw new Error('Missing daily series dates');
for (const field of sourceFields) {
  if (!Array.isArray(series[field]) || series[field].length !== series.dates.length) throw new Error(`Daily array length mismatch: ${field}`);
  if (!series[field].every(Number.isFinite)) throw new Error(`Non-numeric daily input: ${field}`);
}
const isoDate = (value) => {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) throw new Error(`Invalid ISO date: ${value}`);
  const date = new Date(`${value}T00:00:00Z`);
  if (!Number.isFinite(+date) || date.toISOString().slice(0, 10) !== value) throw new Error(`Invalid date: ${value}`);
  return date;
};
const oneDay = 86_400_000;
series.dates.forEach((date, i) => {
  isoDate(date);
  if (i && +isoDate(date) - +isoDate(series.dates[i - 1]) !== oneDay) throw new Error('Daily source dates must be consecutive and unique');
});
const year = Number(args.year ?? series.dates.at(-1).slice(0, 4));
if (!Number.isInteger(year) || year < 1900 || year > 9999) throw new Error('Invalid year');
const sourceIndices = series.dates.flatMap((date, i) => date.startsWith(`${year}-`) ? [i] : []);
if (!sourceIndices.length) throw new Error(`No daily data for ${year}`);
const start = series.dates[sourceIndices[0]];
const cutoff = series.dates[sourceIndices.at(-1)];
if (start !== `${year}-01-01`) throw new Error('Requested year does not start on January 1 in the source');
const requestedThrough = args['requested-through'] ?? cutoff;
isoDate(requestedThrough);
if (requestedThrough < cutoff) throw new Error('Requested-through cannot precede the source year cutoff');

const mix = report.customer_mix;
if (!Array.isArray(mix?.dates) || !Array.isArray(mix.new) || !Array.isArray(mix.returning) || mix.dates.length !== mix.new.length || mix.dates.length !== mix.returning.length) throw new Error('Invalid customer_mix arrays');
const mixByDate = new Map();
mix.dates.forEach((date, i) => {
  isoDate(date);
  if (mixByDate.has(date) || !Number.isFinite(mix.new[i]) || !Number.isFinite(mix.returning[i])) throw new Error('Invalid customer_mix input');
  mixByDate.set(date, [mix.new[i], mix.returning[i]]);
});
const tolerance = 0.011; // One cent of possible source rounding per daily aggregate.
const rows = sourceIndices.map(i => {
  const date = series.dates[i];
  let customerValues = mixByDate.get(date);
  if (!customerValues) {
    if (series.revenue[i] !== 0 || series.orders[i] !== 0) throw new Error(`Missing customer mix on a non-empty day: ${date}`);
    customerValues = [0, 0];
  }
  const row = Object.fromEntries(sourceFields.map(field => [field, series[field][i]]));
  const [firstRevenue, repeatRevenue] = customerValues;
  const ads = row.fb_ads + row.google_ads;
  const cm1 = row.revenue - row.product_cost - row.packaging - row.shipping;
  const cm2 = cm1 - ads;
  const cm3 = cm2 - row.fixed;
  for (const [name, difference] of Object.entries({ ads: ads - row.total_ads, cm2: cm2 - row.profit_without_fixed, cm3: cm3 - row.profit_with_fixed, customerRevenue: row.revenue - firstRevenue - repeatRevenue })) {
    if (Math.abs(difference) > tolerance) throw new Error(`Source reconciliation failed for ${date}: ${name}`);
  }
  return { date, ...row, firstRevenue, repeatRevenue, ads, cm1, cm2, cm3 };
});
const monthCount = Number(cutoff.slice(5, 7));
const cohorts = report.cohort_unit_economics_rows;
if (!Array.isArray(cohorts)) throw new Error('Missing cohort_unit_economics_rows');
const months = Array.from({ length: monthCount }, (_, i) => {
  const key = `${year}-${String(i + 1).padStart(2, '0')}`;
  const matches = cohorts.filter(row => row.cohort_month === key);
  if (matches.length !== 1 || !Number.isInteger(matches[0].new_customers) || matches[0].new_customers < 0) throw new Error(`Missing or duplicate cohort: ${key}`);
  return { key, date: isoDate(`${key}-01`), customers: matches[0].new_customers };
});
const sum = (key) => rows.reduce((total, row) => total + row[key], 0);
const totals = Object.fromEntries(['revenue', 'product_cost', 'packaging', 'shipping', 'ads', 'cm1', 'cm2', 'fixed', 'cm3', 'firstRevenue', 'repeatRevenue', 'orders'].map(key => [key, sum(key)]));
totals.newCustomers = months.reduce((total, month) => total + month.customers, 0);

const resolver = args.dependencies ? createRequire(path.join(path.resolve(args.dependencies), '..', 'artifact-builder.cjs')) : createRequire(import.meta.url);
const { Workbook, SpreadsheetFile } = await import(pathToFileURL(resolver.resolve('@oai/artifact-tool')).href);
const workbook = Workbook.create();
const financial = workbook.worksheets.add('Financie');
const customers = workbook.worksheets.add('Zákazníci');
const daily = workbook.worksheets.add('Denné údaje');
const qaDir = args['qa-dir'] ? path.resolve(args['qa-dir']) : null;
const font = 'Arial';
const currency = '#,##0.00" €";(#,##0.00" €");"–"';
const integer = '#,##0;(#,##0);"–"';
const percentage = '0.0%;(0.0%);"–"';
const colors = { ink: '#172B4D', header: '#253B58', light: '#EAF0F6', border: '#CBD5E1', link: '#008000' };
const column = (index) => { let s = ''; for (let n = index + 1; n; n = Math.floor((n - 1) / 26)) s = String.fromCharCode(65 + (n - 1) % 26) + s; return s; };
function base(sheet, range) {
  sheet.showGridLines = false;
  sheet.getRange(range).format.font = { name: font, size: 11, color: colors.ink };
  sheet.getRange(range).format.rowHeight = 22;
  sheet.getRange(range).format.verticalAlignment = 'center';
}
function header(sheet, address) {
  sheet.getRange(address).format = { fill: colors.header, font: { name: font, size: 11, color: '#FFFFFF', bold: true }, wrapText: true, horizontalAlignment: 'center', verticalAlignment: 'center', rowHeight: 40, borders: { insideVertical: { style: 'thin', color: '#FFFFFF' } } };
}
function title(sheet, address, text, endColumn) {
  sheet.getRange(address).values = [[text]];
  sheet.getRange(address).format.font = { name: font, size: 16, bold: true, color: colors.ink };
  sheet.getRange(`${address}:${endColumn}${address.match(/\d+/)[0]}`).format.borders = { bottom: { style: 'thin', color: colors.border } };
  sheet.getRange(address).format.rowHeight = 28;
}
function band(sheet, address) {
  sheet.getRange(address).format.fill = colors.light;
  sheet.getRange(address).format.font.bold = true;
  sheet.getRange(address).format.borders = { top: { style: 'thin', color: colors.border } };
}
function note(sheet, row, text, widthRange) {
  const [first, last] = widthRange.split(':');
  sheet.getRange(`${first}${row}`).values = [[text]];
  // Notes use a single wide descriptive column or overflow into intentionally empty cells.
  sheet.getRange(`${first}${row}:${last}${row}`).format.font.italic = true;
}

// Immutable aggregate inputs and visibly separate formula columns. No customer identifiers.
const firstDailyRow = 7;
const lastDailyRow = firstDailyRow + rows.length - 1;
base(daily, `A1:Y${lastDailyRow}`);
title(daily, 'A2', 'Denné dáta', 'V');
daily.getRange('A2').format.font.size = 11;
daily.getRange('A3').values = [[`${start} až ${cutoff}; EUR bez DPH; zdroj: ${path.basename(args.input)}`]];
daily.getRange('A3').format.font.italic = true;
daily.getRange('A6:V6').values = [['Dátum', 'Mesiac', 'Objednávky', 'Tržby', 'Produkty', 'Balenie', 'Doprava', 'Meta', 'Google', 'Reklama spolu', 'CM1', 'CM2', 'Fixy', 'CM3', 'Tržby prvých objednávok', 'Tržby opakovaných objednávok', 'Reklama zo zdroja', 'CM2 zo zdroja', 'CM3 zo zdroja', 'Rozdiel CM2', 'Rozdiel CM3', 'Rozdiel tržieb']];
header(daily, 'A6:V6');
daily.getRange('A6:V6').format.rowHeight = 54;
daily.getRange(`A${firstDailyRow}:V${lastDailyRow}`).values = rows.map(row => [isoDate(row.date), null, row.orders, row.revenue, row.product_cost, row.packaging, row.shipping, row.fb_ads, row.google_ads, null, null, null, row.fixed, null, row.firstRevenue, row.repeatRevenue, row.total_ads, row.profit_without_fixed, row.profit_with_fixed, null, null, null]);
for (const [col, formula] of Object.entries({ B: '=MONTH(A7)', J: '=H7+I7', K: '=D7-E7-F7-G7', L: '=K7-J7', N: '=L7-M7', T: '=ROUND(L7-R7,2)', U: '=ROUND(N7-S7,2)', V: '=ROUND(D7-O7-P7,2)' })) {
  daily.getRange(`${col}${firstDailyRow}`).formulas = [[formula]];
  daily.getRange(`${col}${firstDailyRow}:${col}${lastDailyRow}`).fillDown();
}
daily.getRange(`A${firstDailyRow}:A${lastDailyRow}`).setNumberFormat('yyyy-mm-dd');
daily.getRange(`B${firstDailyRow}:C${lastDailyRow}`).setNumberFormat(integer);
daily.getRange(`D${firstDailyRow}:V${lastDailyRow}`).setNumberFormat(currency);
daily.getRange('A:A').format.columnWidth = 13;
daily.getRange('B:C').format.columnWidth = 12;
daily.getRange('D:V').format.columnWidth = 17;
daily.getRange('W:W').format.columnWidth = 3;
daily.getRange('X:Y').format.columnWidth = 18;
daily.getRange('X2').values = [['Nové mesačné kohorty']];
daily.getRange('X2').format.font.bold = true;
daily.getRange('X6:Y6').values = [['Mesiac kohorty', 'Noví zákazníci']];
header(daily, 'X6:Y6');
daily.getRange('A6:Y6').format.rowHeight = 54;
daily.getRange(`X7:Y${6 + months.length}`).values = months.map(month => [month.date, month.customers]);
daily.getRange(`X7:X${6 + months.length}`).setNumberFormat('mmm yyyy');
daily.getRange(`Y7:Y${6 + months.length}`).setNumberFormat(integer);
daily.freezePanes.freezeRows(6);
daily.freezePanes.freezeColumns(1);

// Monthly finance comparison. Every amount/ratio rolls up from daily source cells.
const lastMonthColumn = column(months.length + 1);
const totalColumn = column(months.length + 2);
base(financial, `A1:${totalColumn}36`);
financial.getRange('A:A').format.columnWidth = 3;
financial.getRange('B:B').format.columnWidth = 36;
financial.getRange(`C:${totalColumn}`).format.columnWidth = 17;
title(financial, 'B2', `Vevo – finančné výsledky ${year}`, totalColumn);
financial.getRange('B3').values = [[`${start} až ${cutoff}; ${rows.length} dní; EUR bez DPH. Posledný mesiac je neúplný, ak sa končí pred koncom mesiaca.`]];
financial.getRange('B3').format.font.italic = true;
financial.getRange(`B6:${totalColumn}6`).values = [['Ukazovateľ', ...months.map(month => month.date), 'Spolu']];
financial.getRange(`C6:${lastMonthColumn}6`).setNumberFormat('mmm yyyy');
header(financial, `B6:${totalColumn}6`);
const labels = {
  7: 'Počet dní', 8: 'Tržby bez DPH', 9: 'Produktové náklady', 10: 'Balenie', 11: 'Doprava', 12: 'CM1 pred reklamou',
  13: 'Meta reklama', 14: 'Google reklama', 15: 'Reklama spolu', 16: 'CM2 po reklame', 17: 'Fixné náklady', 18: 'CM3 po fixoch',
  20: 'Tržby / deň', 21: 'Reklama / deň', 22: 'CM2 / deň', 23: 'CM3 / deň', 24: 'Tržby / reklama', 25: 'CM1 / tržby', 26: 'CM2 / tržby'
};
for (const [row, label] of Object.entries(labels)) financial.getRange(`B${row}`).values = [[label]];
const dailyColumns = { 8: 'D', 9: 'E', 10: 'F', 11: 'G', 13: 'H', 14: 'I', 17: 'M' };
for (let m = 0; m < months.length; m++) {
  const col = column(m + 2);
  financial.getRange(`${col}7`).formulas = [[`=COUNTIF('Denné údaje'!$B$7:$B$${lastDailyRow},MONTH(${col}$6))`]];
  for (const [row, sourceCol] of Object.entries(dailyColumns)) {
    financial.getRange(`${col}${row}`).formulas = [[`=SUMIF('Denné údaje'!$B$7:$B$${lastDailyRow},MONTH(${col}$6),'Denné údaje'!$${sourceCol}$7:$${sourceCol}$${lastDailyRow})`]];
    financial.getRange(`${col}${row}`).format.font.color = colors.link;
  }
}
for (const row of [7, ...Object.keys(dailyColumns).map(Number)]) financial.getRange(`${totalColumn}${row}`).formulas = [[`=SUM(C${row}:${lastMonthColumn}${row})`]];
for (let m = 0; m <= months.length; m++) {
  const c = column(m + 2);
  const formulas = { 12: `=${c}8-SUM(${c}9:${c}11)`, 15: `=SUM(${c}13:${c}14)`, 16: `=${c}12-${c}15`, 18: `=${c}16-${c}17`, 20: `=${c}8/${c}7`, 21: `=${c}15/${c}7`, 22: `=${c}16/${c}7`, 23: `=${c}18/${c}7`, 24: `=IF(${c}15=0,"",${c}8/${c}15)`, 25: `=IF(${c}8=0,"",${c}12/${c}8)`, 26: `=IF(${c}8=0,"",${c}16/${c}8)` };
  for (const [row, formula] of Object.entries(formulas)) financial.getRange(`${c}${row}`).formulas = [[formula]];
}
financial.getRange(`C7:${totalColumn}7`).setNumberFormat(integer);
financial.getRange(`C8:${totalColumn}23`).setNumberFormat(currency);
financial.getRange(`C24:${totalColumn}24`).setNumberFormat('0.00"×"');
financial.getRange(`C25:${totalColumn}26`).setNumberFormat(percentage);
for (const row of [8, 12, 16, 18]) band(financial, `B${row}:${totalColumn}${row}`);
financial.getRange(`${totalColumn}7:${totalColumn}26`).format.font.bold = true;
const fixedValues = [...new Set(rows.map(row => row.fixed))];
const fixedNote = fixedValues.length === 1 ? `Fixy sú odhad prevzatý zo zdroja: ${fixedValues[0]} EUR/deň. Nejde o audit účtovných nákladov.` : 'Fixy sú prevzaté z denného odhadu v zdrojovom reporte. Nejde o audit účtovných nákladov.';
const missingStart = new Date(+isoDate(cutoff) + oneDay).toISOString().slice(0, 10);
note(financial, 29, 'CM1 = tržby − produkty − balenie − doprava. CM2 = CM1 − reklama. CM3 = CM2 − fixy.', `B:${totalColumn}`);
note(financial, 30, fixedNote, `B:${totalColumn}`);
note(financial, 31, requestedThrough > cutoff ? `Chýbajú údaje ${missingStart} až ${requestedThrough}. Tieto dni nie sú dopočítané ani odhadnuté.` : 'Obdobie končí posledným dostupným dňom zdrojového reportu.', `B:${totalColumn}`);
note(financial, 32, 'Mesačné porovnanie opisuje súbeh tržieb a spendu. Samo neurčuje príčinný vplyv reklamy.', `B:${totalColumn}`);
note(financial, 33, `Zdroj: ${path.basename(args.input)}, JSON report-dashboard-json, series. Sumy: EUR bez DPH.`, `B:${totalColumn}`);
note(financial, 34, 'Zelené písmo označuje výpočty odkazujúce na denné údaje. Čierne písmo označuje miestne výpočty a zdrojové údaje.', `B:${totalColumn}`);
note(financial, 35, `Report vygenerovaný: ${generatedAt}; časové pásmo zdroj neuvádza.`, `B:${totalColumn}`);
note(financial, 36, `SHA-256 zdroja: ${sourceHash}`, `B:${totalColumn}`);

// Cohort counts use the report's history-based cohort table; the revenue split is a
// separate daily order classification. It is intentionally not paid attribution.
const customerTotalRow = 7 + months.length;
base(customers, `A1:H${30 + 2 * months.length}`);
customers.getRange('A:A').format.columnWidth = 3;
customers.getRange('B:B').format.columnWidth = 16;
customers.getRange('C:H').format.columnWidth = 21;
title(customers, 'B2', `Vevo – zákazníci ${year}`, 'H');
customers.getRange('B3').values = [[`${start} až ${cutoff}; EUR bez DPH. Posledný mesiac obsahuje iba dostupné dni.`]];
customers.getRange('B3').format.font.italic = true;
customers.getRange('B6:H6').values = [['Mesiac', 'Noví zákazníci', 'Tržby prvých objednávok', 'Tržby opakovaných objednávok', 'Tržby spolu', 'Opakované / tržby', 'Reklama / nový zákazník']];
header(customers, 'B6:H6');
customers.getRange('B6:H6').format.rowHeight = 48;
for (let m = 0; m < months.length; m++) {
  const r = 7 + m;
  const fc = column(2 + m);
  customers.getRange(`B${r}`).formulas = [[`='Financie'!${fc}$6`]];
  customers.getRange(`C${r}`).formulas = [[`=SUMIF('Denné údaje'!$X$7:$X$${6 + months.length},B${r},'Denné údaje'!$Y$7:$Y$${6 + months.length})`]];
  for (const [dest, sourceCol] of [['D', 'O'], ['E', 'P']]) customers.getRange(`${dest}${r}`).formulas = [[`=SUMIF('Denné údaje'!$B$7:$B$${lastDailyRow},MONTH($B${r}),'Denné údaje'!$${sourceCol}$7:$${sourceCol}$${lastDailyRow})`]];
  customers.getRange(`F${r}`).formulas = [[`=SUM(D${r}:E${r})`]];
  customers.getRange(`G${r}`).formulas = [[`=IF(F${r}=0,"",E${r}/F${r})`]];
  customers.getRange(`H${r}`).formulas = [[`=IF(C${r}=0,"",'Financie'!${fc}15/C${r})`]];
  customers.getRange(`B${r}:E${r}`).format.font.color = colors.link;
  customers.getRange(`H${r}`).format.font.color = colors.link;
}
customers.getRange(`B7:B${customerTotalRow - 1}`).setNumberFormat('mmm yyyy');
customers.getRange(`B${customerTotalRow}`).values = [['Spolu']];
for (const col of ['C', 'D', 'E', 'F']) customers.getRange(`${col}${customerTotalRow}`).formulas = [[`=SUM(${col}7:${col}${customerTotalRow - 1})`]];
customers.getRange(`G${customerTotalRow}`).formulas = [[`=IF(F${customerTotalRow}=0,"",E${customerTotalRow}/F${customerTotalRow})`]];
customers.getRange(`H${customerTotalRow}`).formulas = [[`=IF(C${customerTotalRow}=0,"",'Financie'!${totalColumn}15/C${customerTotalRow})`]];
customers.getRange(`C7:C${customerTotalRow}`).setNumberFormat(integer);
customers.getRange(`D7:F${customerTotalRow}`).setNumberFormat(currency);
customers.getRange(`G7:G${customerTotalRow}`).setNumberFormat(percentage);
customers.getRange(`H7:H${customerTotalRow}`).setNumberFormat(currency);
band(customers, `B${customerTotalRow}:H${customerTotalRow}`);
// Visible chart labels are a formula-linked reshaping of the date column.
const chartHelperRow = customerTotalRow + 23;
customers.getRange(`B${chartHelperRow}:D${chartHelperRow}`).values = [['Mesiac grafu', 'Tržby prvých objednávok', 'Tržby opakovaných objednávok']];
header(customers, `B${chartHelperRow}:D${chartHelperRow}`);
customers.getRange(`B${chartHelperRow}:D${chartHelperRow}`).format.rowHeight = 48;
for (let i = 0; i < months.length; i++) customers.getRange(`B${chartHelperRow + 1 + i}:D${chartHelperRow + 1 + i}`).formulas = [[`=TEXT(B${7 + i},"mmm yyyy")`, `=D${7 + i}`, `=E${7 + i}`]];
customers.getRange(`C${chartHelperRow + 1}:D${chartHelperRow + months.length}`).setNumberFormat(currency);
const chart = customers.charts.add('bar', customers.getRange(`B${chartHelperRow}:D${chartHelperRow + months.length}`));
chart.barOptions.direction = 'column';
chart.barOptions.grouping = 'stacked';
chart.barOptions.gapWidth = 120;
chart.title = 'Tržby prvých a opakovaných objednávok';
chart.titleTextStyle.typeface = font;
chart.titleTextStyle.fontSize = 15;
chart.legend = { position: 'bottom', textStyle: { typeface: font, fontSize: 12 } };
chart.xAxis = { axisType: 'textAxis', textStyle: { typeface: font, fontSize: 11 } };
chart.yAxis = { numberFormatCode: '#,##0" €"', numberFormatSourceLinked: false, textStyle: { typeface: font, fontSize: 11 } };
chart.series.items[0].fill = '#547DA6';
chart.series.items[1].fill = '#D49445';
chart.setPosition(`B${customerTotalRow + 2}`, `H${customerTotalRow + 16}`);
const notesStart = customerTotalRow + 17;
note(customers, notesStart, `Posledný stĺpec grafu: ${cutoff.slice(0, 7)}, dni 1.–${Number(cutoff.slice(8))}.`, 'B:H');
note(customers, notesStart + 1, 'Nový zákazník = prvá známa objednávka v dostupnej zákazníckej histórii reportu; mesačná kohorta.', 'B:H');
note(customers, notesStart + 2, 'Prvé a opakované tržby sú klasifikácia objednávok. Tržby z ďalšej objednávky nového zákazníka už patria medzi opakované.', 'B:H');
note(customers, notesStart + 3, 'Podiel opakovaných tržieb môže rásť aj pri poklese prvých tržieb. Nie je dôkazom rastu retencie ani účinku reklamy.', 'B:H');
note(customers, notesStart + 4, 'Reklama / nový zákazník je blended proxy zo všetkého spendu; nejde o atribučný CAC ani inkrementálny CAC.', 'B:H');
note(customers, notesStart + 5, 'Zdroj: report-dashboard-json, cohort_unit_economics_rows a customer_mix. Bez zákazníckych identifikátorov.', 'B:H');

// Check computed workbook outputs against independent JS aggregation of raw inputs.
const expectedCells = [[financial, `${totalColumn}7`, rows.length], [financial, `${totalColumn}8`, totals.revenue], [financial, `${totalColumn}9`, totals.product_cost], [financial, `${totalColumn}10`, totals.packaging], [financial, `${totalColumn}11`, totals.shipping], [financial, `${totalColumn}15`, totals.ads], [financial, `${totalColumn}16`, totals.cm2], [financial, `${totalColumn}17`, totals.fixed], [financial, `${totalColumn}18`, totals.cm3], [customers, `C${customerTotalRow}`, totals.newCustomers], [customers, `D${customerTotalRow}`, totals.firstRevenue], [customers, `E${customerTotalRow}`, totals.repeatRevenue]];
for (const [sheet, address, expected] of expectedCells) {
  const actual = sheet.getRange(address).values[0][0];
  if (typeof actual !== 'number' || Math.abs(actual - expected) > 0.000001) throw new Error(`Calculated output mismatch at ${sheet.name}!${address}: ${actual} vs ${expected}`);
}
for (let m = 0; m < months.length; m++) {
  const actual = customers.getRange(`C${m + 7}`).values[0][0];
  if (actual !== months[m].customers) throw new Error(`Monthly cohort count mismatch: ${months[m].key}`);
}
const inspection = await workbook.inspect({ kind: 'table', range: `Financie!B6:${totalColumn}26`, include: 'values,formulas', tableMaxRows: 22, tableMaxCols: 14, maxChars: 10000 });
const errors = await workbook.inspect({ kind: 'match', searchTerm: '#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A|#NUM!|#NULL!|#SPILL!|#CALC!', options: { useRegex: true, maxResults: 100 }, summary: 'Final formula error scan' });
if (/#REF!|#DIV\/0!|#VALUE!|#NAME\?|#N\/A|#NUM!|#NULL!|#SPILL!|#CALC!/.test(errors.ndjson)) throw new Error(`Formula errors found: ${errors.ndjson}`);
if (qaDir) {
  await fs.mkdir(qaDir, { recursive: true });
  await fs.writeFile(path.join(qaDir, 'inspection.ndjson'), `${inspection.ndjson}\n${errors.ndjson}\n`);
  const views = [['finance', 'Financie', `A1:${totalColumn}37`], ['customers', 'Zákazníci', `A1:H${notesStart + 5}`], ['customer_chart_inputs', 'Zákazníci', `B${chartHelperRow}:D${chartHelperRow + months.length}`], ['daily_finance', 'Denné údaje', 'A1:L16'], ['daily_customers', 'Denné údaje', 'M1:Y16']];
  for (const [name, sheetName, range] of views) {
    const preview = await workbook.render({ sheetName, range, scale: 1.5, format: 'png' });
    await fs.writeFile(path.join(qaDir, `${name}.png`), new Uint8Array(await preview.arrayBuffer()));
  }
}
await fs.mkdir(path.dirname(path.resolve(args.output)), { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(path.resolve(args.output));
console.log(JSON.stringify({ output: path.resolve(args.output), source: path.basename(args.input), generatedAt, sourceHash, start, cutoff, requestedThrough, days: rows.length, months: months.length, totals, checks: { dailySourceReconciliation: true, monthlyCohorts: true, workbookTotals: true, formulaErrors: 0, chartSeries: chart.series.items.map(series => ({ formula: series.formula, categoryFormula: series.categoryFormula })) }, qaDir }));

"""Render the private VEVO country analysis from frozen aggregate evidence."""
import argparse
import json
from pathlib import Path


def fmt(value, decimals=2):
    if value is None:
        return '—'
    return f'{value:,.{decimals}f}'.replace(',', ' ').replace('.', ',')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input-dir', type=Path, default=Path(__file__).resolve().parents[1] / 'data/country-analysis-20261007/aggregates')
    args = parser.parse_args()
    def read(name):
        return json.loads((args.input_dir / name).read_text(encoding='utf-8'))
    comparison = read('vevo_country_comparison_20261007.json')
    orders = read('vevo_country_orders_20261007.json')
    meta = read('vevo_meta_performance_20261007.json')
    coverage = read('vevo_country_order_coverage_20261007.json')
    lookup = {(r['month'], r['country']): r for r in comparison['rows']}
    aug, sep = lookup['2026-08', 'SK'], lookup['2026-09', 'SK']
    def delta(a, b):
        return 100 * (b / a - 1)
    lines = [
        '# VEVO: SK, CZ a HU — august a september 2026', '',
        'Vypracované 7. 10. 2026. Posledný kompletný denný report: 6. 10. 2026. '
        'Október 1.–6. je samostatný priebežný signál. Ide o analýzu, bez zmien kampaní či produkcie.', '',
        '**Hlavný záver:** SK má výrazne vyšší reklamný rozpočet voči rastu tržieb a menšie košíky. '
        'CZ/HU majú slabú ekonomiku prvého nákupu. HU navyše finančný report výrazne podhodnocuje '
        'pre chýbajúcu maďarskú dobierku; korekcia túto krajinu sama osebe nerobí ziskovou.', '',
        'Všetky tržby sú za tovar bez DPH a dopravy v EUR podľa kurzov reportingu. '
        'Objednávky znamenajú reportom uznané objednávky plus výslovne overené odoslané HU dobierky; '
        'odoslaná dobierka nie je doklad prijatej hotovosti. MER = všetky tržby shopu / reklama, '
        'nie nameraný ROAS kampane. Tabuľka zachováva zaokrúhlenie pôvodných uznaných tržieb; '
        'pri HU k nim pridáva chýbajúce dobierky z priameho auditu. Samostatný celý live výpočet HU sa líši o jeden cent.', '',
        '| Mesiac | Shop | Objednávky | Tržby za tovar € | Priemerný košík € | Meta + Google € | MER |',
        '|---|---|---:|---:|---:|---:|---:|']
    for row in comparison['rows']:
        if row['month'] == '2026-10':
            continue
        lines.append(f"| {row['month']} | {row['country']} | {row['corrected_eligible_orders']} | "
                     f"{fmt(row['corrected_net_merchandise_revenue'])} | {fmt(row['aov'])} | "
                     f"{fmt(row['ads_spend'])} | {fmt(row['blended_mer'])} |")
    lines += ['', 'CZ v auguste nemalo uznané platené objednávky (osem ďalších odoslaných záznamov malo nulovú hodnotu). '
              'HU malo jednu odoslanú dobierku. Prvá pozorovaná Meta útrata v tomto období bola HU 12. 9. a CZ 14. 9.; '
              'nejde o dve etablované krajiny s porovnateľným augustovým reklamným základom.', '',
              '**SK — čo sa zmenilo**', '',
              f"Meta spend vzrástol o {fmt(delta(aug['meta_spend'], sep['meta_spend']), 1)} %, "
              f"tržby o {fmt(delta(aug['corrected_net_merchandise_revenue'], sep['corrected_net_merchandise_revenue']), 1)} % "
              f"a objednávky o {fmt(delta(aug['corrected_eligible_orders'], sep['corrected_eligible_orders']), 1)} %. "
              f"Košík klesol o {fmt(-delta(aug['aov'], sep['aov']), 1)} %. "
              f"Modelovaný príspevok po tovare, balení/doprave a reklame klesol z {fmt(aug['estimated_contribution_after_ads_before_fixed'])} € "
              f"na {fmt(sep['estimated_contribution_after_ads_before_fixed'])} €. To sú peniaze zostávajúce na spoločnú réžiu, nie čistý zisk.", '',
              '| SK ukazovateľ | August | September |', '|---|---:|---:|']
    for label, key in [('Nové zákaznícke objednávky', 'new_customer_orders'), ('Vracajúce sa objednávky', 'returning_customer_orders'),
                       ('Podiel vracajúcich sa objednávok %', 'returning_order_share_pct'), ('Príspevok pred reklamou na objednávku €', 'modeled_cm1_per_order_eur')]:
        vals = [orders['periods'][m]['currency_shops']['SK'][key] for m in ['2026-08', '2026-09']]
        precision = 0 if key in ('new_customer_orders', 'returning_customer_orders') else 2
        lines.append(f'| {label} | {fmt(vals[0], precision)} | {fmt(vals[1], precision)} |')
    lines += ['', 'Vracajúce sa objednávky absolútne neklesli. Ich podiel sa znížil pri raste nových zákazníkov. '
              'Zároveň klesol košík nových aj vracajúcich sa zákazníkov; nejde iba o zmenu mixu.', '',
              '| Basket / SK | August | September |', '|---|---:|---:|']
    for key, label, measure in [
        ('first_customer_order', 'Košík nového zákazníka €', 'aov_net_item_eur'),
        ('returning_customer_order', 'Košík vracajúceho sa zákazníka €', 'aov_net_item_eur'),
        ('first_customer_order', 'Príspevok prvého nákupu pred reklamou €', 'modeled_cm1_per_order_eur'),
        ('contains_essence_sample_set_9x10', 'Objednávky s Essence 9×10 ml', 'orders')]:
        vals = [orders['periods'][m]['basket_cohorts']['SK'][key][measure] for m in ['2026-08', '2026-09']]
        precision = 0 if measure == 'orders' else 2
        lines.append(f'| {label} | {fmt(vals[0], precision)} | {fmt(vals[1], precision)} |')
    lines += ['', '**CZ a HU — samostatné výsledky**', '']
    for country in ['CZ', 'HU']:
        row = lookup['2026-09', country]
        lines.append(f"{country}: {row['corrected_eligible_orders']} objednávok, košík {fmt(row['aov'])} €, "
                     f"tržby {fmt(row['corrected_net_merchandise_revenue'])} €, reklama {fmt(row['ads_spend'])} €. "
                     f"Samotné tržby za tovar mínus reklama sú {fmt(row['revenue_less_ads_before_all_costs'])} €, "
                     'ešte pred nákladmi na tovar. Nie je to účtovný hospodársky výsledok.')
        lines.append('')
    cz = orders['periods']['2026-09']['basket_cohorts']['CZ']
    lines += [f"V CZ obsahovalo Essence 9×10 ml {cz['contains_essence_sample_set_9x10']['orders']} z "
              f"{lookup['2026-09', 'CZ']['corrected_eligible_orders']} objednávok. Samotné vzorkové košíky nechali "
              f"pred reklamou približne {fmt(cz['only_essence_sample_set_9x10']['modeled_cm1_per_order_eur'])} € na objednávku. "
              f"Modelovaný CZ príspevok po reklame je {fmt(lookup['2026-09', 'CZ']['estimated_contribution_after_ads_before_fixed'])} € pred fixmi.", '',
              'HU kompletnú maržu neuvádzam: chýbajúce objednávky nemajú nákladové riadky v pôvodnom reporte. '
              'Mix vzoriek z pôvodných HU kartových objednávok sa nesmie vydávať za mix všetkých HU objednávok.', '',
              '**Konkrétne Meta kampane v septembri**', '',
              'Presná akcia `offsite_conversion.fb_pixel_purchase`, explicitné okno `7d_click`, dátum reklamného zobrazenia. '
              'Hodnoty Meta majú vlastný základ udalosti, nie preukázane rovnaký DPH/doprava základ ako tržby shopu. '
              'Jednotlivé akcie ani 1d-view sa nesčítavajú.', '',
              '| Kampaň | Spend € | Nákupy 7d-click | CPA € | Meta ROAS | LPV / link kliky |',
              '|---|---:|---:|---:|---:|---:|']
    campaigns = [r for r in meta['campaign_monthly'] if r['date_start'] == '2026-09-01']
    for row in sorted(campaigns, key=lambda r: -r['spend']):
        lines.append(f"| {row['campaign_name']} | {fmt(row['spend'])} | {fmt(row['purchases'], 0)} | "
                     f"{fmt(row['purchase_cpa'])} | {fmt(row['platform_purchase_roas'])} | {fmt(row['lpv_per_link_click_pct'], 1)} % |")
    lines += ['', 'UGC Contest mal útratu 10.–20. septembra. Pri kontrole 7. októbra boli všetky jeho vrátené reklamy '
              'v stave ADSET_PAUSED. Cieľ bol predaj/Purchase, landing kategória pracích parfumov. '
              'Slabý pomer nameraných LPV ku klikom je diagnostický signál; sám nedokazuje pomalý web, chybu checkoutu ani nekvalitné kliky.', '',
              '| Meta SK lievik | August | September |', '|---|---:|---:|']
    meta_sk = {r['date_start'][:7]: r for r in meta['country_monthly'] if r['country'] == 'SK'}
    for key, label in [('cpm', 'CPM €'), ('link_cpc', 'CPC link €'), ('link_ctr_pct', 'Link CTR %'), ('frequency', 'Frekvencia'),
                       ('lpv_per_link_click_pct', 'LPV / link kliky %'), ('purchases_per_lpv_pct', '7d-click nákupy / LPV %')]:
        lines.append(f"| {label} | {fmt(meta_sk['2026-08'][key])} | {fmt(meta_sk['2026-09'][key])} |")
    lines += ['', 'CPM nestúplo. Dáta nepodporujú vysvetlenie, že hlavný problém je prudké zdraženie reklamnej aukcie. '
              'Frekvencia rastie a CTR mierne klesá; únava kreatív je možná, ale týmto auditom nie je kauzálne dokázaná. '
              'Pomer Meta nákupov ku LPV nie je skutočná webová konverzná miera.', '',
              '**Opakované nákupy**', '',
              '| SK akvizičná kohorta | Zákazníci | Opakovaný nákup do 30 dní | Príspevok do 30 dní / zákazník € |',
              '|---|---:|---:|---:|']
    for key, label in [('SK_acquired_2026-08_mature30d', 'August'), ('SK_acquired_2026-09-01_06_mature30d', '1.–6. september')]:
        row = orders['retention_cohorts']['cohorts'][key]['all']
        lines.append(f"| {label} | {row['customers']} | {fmt(row['repeat_customer_pct_within_observed_up_to_30d'], 1)} % | "
                     f"{fmt(row['observed_up_to_30d_cm1_per_customer_eur'])} |")
    lines += ['', 'Úplné 30-dňové kohorty nepreukázali kolaps retencie. Septembrová porovnateľná vzorka je malá. '
              'Väčšina septembrových zákazníkov ešte nemá celých 30 dní; LTV ani neskorší payback tým nie sú dokázané.', '',
              '**Potvrdené chyby a obmedzenia reportingu**', '',
              '| Mesiac | Nezapočítané odoslané HU dobierky | Chýbajúce tržby € |', '|---|---:|---:|']
    for month in ['2026-08', '2026-09', '2026-10']:
        row = lookup[month, 'HU']
        lines.append(f"| {month} | {row['omitted_cod_orders']} | {fmt(row['omitted_cod_revenue'])} |")
    lines += ['', 'Príčina: `realized_revenue.cod_payment_ids` neobsahuje HU ID 16 a textové vzory nerozpoznajú '
              '`Utánvétes fizetés`. Výrobný dashboard už túto platbu pozná, finančná klasifikácia nie. '
              f"Priame čítanie pokrylo {coverage['order_count']} objednávok a došlo za začiatok obdobia. "
              'SK/CZ počty a tržby sa zhodujú s archívom. HU zaokrúhlenie sa medzi Decimal a pôvodným float výpočtom líši o cent.', '',
              'Kampaňové objednávky v pôvodnom reporte sú modelované 60 % podielom klikov + 40 % podielom spendu. '
              'Google sa v pôvodných geo výpočtoch rozpočítava na objednávky. Preto som použil priamy Meta country breakdown '
              'a Google API. Google mal iba Brand kampaň s aktuálnou URL vevo.sk; celú jej útratu priraďujeme SK shopu. '
              'Drobné zahraničné Google kliky tým nie sú vydávané za akvizíciu zahraničného shopu. Historické URL nie sú overené.', '',
              'Nákladové fallbacky majú vysoké zastúpenie pri zahraničných tituloch. '
              'V septembri sa odhad 35 % marže týka '
              f"{fmt(orders['periods']['2026-09']['currency_shops']['CZ']['cost_fallback_revenue_share_pct'], 1)} % CZ tržieb a "
              f"{fmt(orders['periods']['2026-09']['currency_shops']['HU']['cost_fallback_revenue_share_pct'], 1)} % tržieb uznanej HU kartovej podmnožiny. "
              'Tieto marže nie sú účtovné skutočnosti. EUR prepočet používa fixné CZK 0,04 a HUF 0,0025. '
              'Balenie + čistá doprava sú modelovaných 0,50 €/objednávku; spoločná réžia 70 €/deň je odhad. '
              'Septembrové dodatočné náklady dobropisov 1 € neboli svojvoľne rozdelené na krajiny.', '',
              '**Platby — doplnková stopa, nie potvrdená chyba brány**', '',
              'Počet expirovaných alebo nezaplatených objednávok nepredstavuje počet unikátnych zákazníkov ani '
              'technických zlyhaní brány. Existujú opakované pokusy a úmyselné opustenia. '
              'HU má v septembri expirované kartové objednávky; overenie dôvodu vyžaduje samostatné platobné udalosti. '
              'Audit nerobil skúšobné platby.', '',
              '**1.–6. október: samostatný krátky signál**', '',
              '| Shop | Objednávky | Tržby € | Reklama € | MER |', '|---|---:|---:|---:|---:|']
    for row in comparison['rows']:
        if row['month'] == '2026-10':
            lines.append(f"| {row['country']} | {row['corrected_eligible_orders']} | {fmt(row['corrected_net_merchandise_revenue'])} | "
                         f"{fmt(row['ads_spend'])} | {fmt(row['blended_mer'])} |")
    lines += ['', 'Krátke obdobie a otvorené atribučné okno neumožňujú vyhlásiť trvalé zlepšenie.', '',
              '**Odporúčané poradie ďalších krokov**', '',
              '1. Samostatnou opravou doplniť overenú HU dobierku do finančnej klasifikácie, prepočítať históriu a overiť SK/CZ/HU. '
              'Doplniť chýbajúce produktové náklady; nepoužívať modelované kampaňové objednávky ako namerané nákupy.',
              '2. Do overenia jednotkovej ekonomiky nezvyšovať CZ/HU rozpočty. Hodnotiť tržby, príspevok a opakované nákupy každého shopu zvlášť.',
              '3. UGC Contest neobnovovať v rovnakej podobe. Oddeliť akvizíciu vzoriek od plných balení a hodnotiť každú ponuku podľa príspevku a paybacku.',
              '4. Skontrolovať mobilný prechod reklama → konkrétna ponuka → košík a konzistentné meranie LPV/Purchase. '
              'Overiť platobné udalosti pri HU/CZ kartách; nepovažovať expiráciu automaticky za technickú poruchu.',
              '5. Vyhodnotiť plne dozreté 30/60-dňové kohorty vzorka → plné balenie. Retenčné výnosy nepripisovať vopred.', '',
              '**Dôkaz a reprodukovateľnosť**', '',
              'Zdroj: súkromný S3 archív `daily-reports/vevo/20261006T231826Z/`, aktuálne read-only Meta/Google API a '
              'VEVO GraphQL. Presné snapshoty, kontrolné súčty, metodika a query sú v sprievodných JSON súboroch. '
              'Detailné dáta nie sú určené do verejného Git repozitára. Generátory a technický handoff sú verzované v Gite.', '',
              'Žiadne kampane, objednávky, platby, ceny, služby ani scheduler neboli zmenené. '
              'Nevznikol lokálny dev server, worker alebo tunel. Finite auditné procesy skončili.', '']
    output = args.input_dir / 'VEVO_analyza_SK_CZ_HU_2026-08_09.md'
    output.write_text('\n'.join(lines), encoding='utf-8')
    print(output)


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""Build and validate VEVO batch 49 household-material-system articles."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from build_batch_45_fabric_structures import (
    BASE,
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    article_hrefs,
    callout,
    esc,
    faq,
    fetch_status,
    related_links,
    source_box,
    table,
    visible_text,
)


PUBLISH_DATE = "2026-08-27"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-49-candidates-2026-08-27.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-49-2026-08-27-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-49-2026-08-27-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_COLOR = "https://members.aatcc.org/store/tm61/495/"
AATCC_SOIL = "https://members.aatcc.org/store/tm215/3848/"
COTTONWORKS_WEAVING = "https://cottonworks.com/wp-content/uploads/2023/03/Weaving-101.pdf"
V_AND_A_DAMASK = "https://www.vam.ac.uk/blog/museum-life/cataloguing-the-courtaulds-textiles-design-library-spotlight-on-10-objects"
KAPOK_REVIEW = "https://pmc.ncbi.nlm.nih.gov/articles/PMC9699385/"
CEIBA_REVIEW = "https://pmc.ncbi.nlm.nih.gov/articles/PMC8876852/"
NATURAL_FIBRE_REVIEW = "https://pmc.ncbi.nlm.nih.gov/articles/PMC12926875/"
FAO_HARD_FIBRES = "https://www.fao.org/markets-and-trade/commodities-overview/fibres/jute-and-hard-fibres/en"
FAO_SISAL = "https://www.fao.org/4/Y1873E/y1873e09.htm"
FAO_SISAL_USES = "https://www.fao.org/UNfao/bodies/ccp/hfj/98/x0129e.htm"
ISO_NONWOVEN = "https://www.iso.org/standard/90537.html?browse=tc"
VLIESELINE_BROCHURE = "https://www.freudenberg-pm.com/-/media/Files/Global%20files%20for%20all%20sites/Brochures/Vlieseline_Gesamtbroschuere_2023_EN.pdf"
VLIESELINE_FUNCTION = "https://www.freudenberg-pm.com/en/markets-solutions/kidswear"
VLIESELINE_CARE = "https://www.vlieseline.com/Producten/fusible-interlinings/H-180"
ASTM_INTERLINING = "https://store.astm.org/standards/d2724"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_BEDDING = "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"
ARTICLE_BEDDING_FREQ = "/n/ako-casto-prat-postelne-pradlo"
ARTICLE_THREAD_COUNT = "/n/pocet-niti-pri-oblieckach-co-znamena-thread-count-a-co-o-kvalite-nehovori"
ARTICLE_FEATHER_PILLOW = "/n/ako-vyprat-paperovy-vankus-kompletny-sprievodca"
ARTICLE_TRAVEL_PILLOW = "/n/ako-prat-cestovny-vankus-a-navleky-po-dovolenke"
ARTICLE_RUG = "/n/ako-vycistit-koberec-kompletny-sprievodca-cistenim-a-udrzbou"
ARTICLE_WASHABLE_RUG = "/n/ako-prat-koberce-v-pracke-casto-kladene-otazky-a-odpovede"
ARTICLE_DOORMAT = "/n/ako-vycistit-rohozku-a-textilie-v-predsieni-od-posypovej-soli"
ARTICLE_BLAZER = "/n/ako-prat-sako-doma-a-kedy-ho-radsej-dat-do-cistiarne"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"

LAUNDRY_PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
LAUNDRY_PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
LAUNDRY_CATEGORY_NAME = "Pracie gély"
LAUNDRY_CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"
RUG_PRODUCT_NAME = "The Pink Stuff odstraňovač škvŕn na koberce 500 ml"
RUG_PRODUCT_URL = "/p-649/the-pink-stuff-odstranovac-skvrn-na-koberce-500-ml"
CLEANING_CATEGORY_NAME = "Čistiace prostriedky"
CLEANING_CATEGORY_URL = "/c/vevo-home-care/upratovanie/cistiace-prostriedky"


def common_sections(article: dict[str, object]) -> list[dict[str, object]]:
    """Return safety sections that remain valid outside ordinary machine washing."""
    return [
        {
            "heading": article["identity_heading"],
            "paragraphs": [
                f"Pri názve {article['name']} treba oddeliť surovinu, konštrukciu a hotový výrobok. {article['identity_detail']} Označenie na prednej strane obalu preto nemusí opisovať všetky vrstvy ani povedať, či sa predmet smie ponoriť do vody. Rozhodujú aj švy, podklad, výplň, lepidlo, farbivo a povrchová úprava.",
                f"Európske pravidlá pre názvy textilných vlákien pomáhajú overiť deklarované zloženie, ale nenahrádzajú ošetrovací návod. {article['identity_boundary']} Pri rozhodovaní si položte dve samostatné otázky: z čoho je viditeľná časť a čo drží tvar celého predmetu. Bez druhej odpovede nemožno bezpečne zvoliť vodu, teplo ani mechaniku.",
            ],
        },
        {
            "heading": "Ako čítať štítok a návod bez nebezpečných skratiek",
            "paragraphs": [
                f"Najprv si zapíšte vláknové zloženie a potom všetky symboly prania, bielenia, sušenia, žehlenia a profesionálneho čistenia. Pri tomto výrobku navyše overte {article['label_focus']}. Symbol sa vzťahuje na celý kus v stave, v akom sa predáva, nie iba na materiál, ktorý vidíte na povrchu.",
                f"Keď štítok chýba, z fotografie alebo dotyku nemožno spoľahlivo dopočítať bezpečný program. {article['missing_label']} Skrytá skúška môže odhaliť uvoľňovanie farby, zmenu omaku alebo reakciu lepidla, ale nie je povolením na vypranie celého predmetu. Pri hodnotnom, rozmernom alebo konštrukčne zložitom kuse je presný údaj výrobcu dôležitejší než všeobecná rada.",
            ],
        },
        {
            "heading": "Suchá kontrola pred vodou ušetrí zbytočný zásah",
            "paragraphs": [
                f"Pred čistením predmet odfoťte pri dennom rozptýlenom svetle a prezrite z líca, rubu aj na hranách. Hľadajte {article['dry_check']}. Prach odstráňte iba spôsobom primeraným povrchu; poškodenú časť nevysávajte bez ochrany a neohýbajte ju len preto, aby ste zistili, či ešte drží.",
                f"Rozlíšte voľnú nečistotu, lokálnu škvrnu, pach a konštrukčnú poruchu. {article['damage_boundary']} Voda je užitočná iba pri probléme, ktorý sa ňou dá bezpečne odstrániť. Ak je príčinou pretrhnutá niť, oddelená vrstva, staré lepidlo alebo deformovaný podklad, ďalšie namočenie chybu neopraví a môže ju rozšíriť.",
            ],
        },
        {
            "heading": "Skrytá skúška: čo dokáže ukázať a čo nie",
            "paragraphs": [
                f"Na nenápadnom mieste použite rovnaký roztok, koncentráciu, čas a spôsob odsatia, aké zvažujete pre viditeľnú plochu. Sledujte prenos farby na bielu handričku, zmenu lesku, tvrdnutie, lepkavosť a vznik ostrého okraja po vyschnutí. {article['test_focus']} Výsledok hodnotíte až po úplnom vysušení, nie počas dočasného stmavnutia mokrého povrchu.",
                "Takáto skúška neoverí všetky miesta výrobku. Iné farbivo môže byť na bordúre, iné lepidlo pri šve a iný podklad v strede. Neurčí ani vnútornú vlhkosť či dlhodobú pevnosť spoja. Ak skúšobné miesto zmení farbu, omak alebo tvar, nepokračujte silnejším roztokom; výsledok je varovanie, nie výzva na zvýšenie dávky.",
            ],
        },
        {
            "heading": "Voda, chémia, pohyb a teplo pôsobia naraz",
            "paragraphs": [
                f"Poškodenie zvyčajne nevytvorí jediný faktor. Voda môže napučať vlákno, prostriedok zmeniť povrchové napätie, trenie presunúť nečistotu a teplo urýchliť schnutie aj chemické zmeny. Pri {article['locative']} je kritická kombinácia {article['combined_risk']}. Preto sa bezpečnosť nedá odvodiť iba z nízkej teploty alebo zo slova jemný na obale produktu.",
                f"Použite najmenší zásah, ktorý rieši konkrétnu nečistotu, a rešpektujte kontaktný čas aj oplach uvedený výrobcom. {article['chemistry_boundary']} Nemiešajte rôzne čističe a nepridávajte chlórové bielidlo, rozpúšťadlo, kyselinu ani zásadu bez výslovného potvrdenia kompatibility. Zvyšok produktu môže po vyschnutí priťahovať prach, meniť omak alebo dráždiť pokožku.",
            ],
        },
        {
            "heading": "Úplné vysušenie je súčasť čistenia, nie posledná formalita",
            "paragraphs": [
                f"Povrch môže vyzerať suchý skôr než šev, výplň, podklad alebo lepená vrstva. {article['drying_detail']} Prúdenie vzduchu má dosiahnuť všetky strany bez prudkého lokálneho ohrievania. Pred uložením porovnajte chladnejšie a hrubšie miesta a skontrolujte, či sa po krátkom uzavretí nevracia vlhký pach.",
                f"Radiátor, horúci fén, intenzívne slnko alebo sušička nie sú univerzálnou skratkou. {article['heat_boundary']} Ak schnutie trvá nezvyčajne dlho, problémom môže byť nasiaknutá vnútorná vrstva alebo nevhodné prostredie. Predmet nepoužívajte ani neuzatvárajte do obalu dovtedy, kým nie je suchý v celej hrúbke.",
            ],
        },
        {
            "heading": "Kedy čistenie zastaviť a riešiť opravu alebo odborníka",
            "paragraphs": [
                f"Domáci zásah ukončite, ak sa objaví {article['stop_signs']}. Zvyšnú vlhkosť bezpečne odsajte bez trenia, predmet podoprite a zdokumentujte stav. Opakovaný cyklus bez poznania príčiny zvyšuje riziko, že sa lokálna zmena rozšíri alebo sa stratí informácia dôležitá pre reklamáciu.",
                f"Odborník má zmysel vtedy, keď treba odlíšiť škvrnu od degradácie materiálu, rozobrať vrstvy alebo zvoliť kontrolované čistenie. {article['professional_boundary']} Pri novom výrobku najprv kontaktujte predajcu; pri historickom alebo sentimentálnom predmete konzervátora. Cieľom nemusí byť dokonalý vzhľad za každú cenu, ale zachovanie funkcie a materiálu.",
            ],
        },
    ]


def commercial_blocks(article: dict[str, object]) -> str:
    return f"""
<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">{esc(article['product_heading'])}</h2>
<p>{article['product_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{esc(article['product_name'])}</h3>
<p>{article['product_text']}</p>
<p><strong>Dôležitá hranica:</strong> {article['product_limit']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="{article['product_url']}">Pozrieť produkt</a></p>
</div>
</div>
<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">{esc(article['category_heading'])}</h2>
<p>{article['category_intro']}</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">{esc(article['category_name'])}</h3>
<p>{article['category_text']}</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="{article['category_url']}">Pozrieť kategóriu</a></p>
</div>
</div>
""".strip()


def render_article(article: dict[str, object]) -> str:
    parts = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['answer']}</p>",
        f"<p>{article['intro']}</p>",
        callout("Najdôležitejšie zistenia v skratke", article["quick"]),
        f"<h2>{esc(article['overview_heading'])}</h2>",
    ]
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["overview"])
    parts.append(f"<h2>{esc(article['table1_heading'])}</h2>")
    parts.append(f"<p>{article['table1_intro']}</p>")
    parts.append(table(article["table1_headers"], article["table1_rows"]))
    for section in list(article["sections"]) + common_sections(article):
        parts.append(f"<h2>{esc(section['heading'])}</h2>")
        parts.extend(f"<p>{paragraph}</p>" for paragraph in section["paragraphs"])
        if section.get("callout"):
            note = section["callout"]
            parts.append(
                callout(
                    note["title"],
                    note["items"],
                    background=note.get("background", "#fffaf5"),
                    border=note.get("border", "#e6ded2"),
                )
            )
    parts.append(f"<h2>{esc(article['table2_heading'])}</h2>")
    parts.append(f"<p>{article['table2_intro']}</p>")
    parts.append(table(article["table2_headers"], article["table2_rows"]))
    parts.append(f"<h2>{esc(article['steps_heading'])}</h2>")
    parts.append("<ol>" + "".join(f"<li>{item}</li>" for item in article["steps"]) + "</ol>")
    parts.append(callout("Čo si skontrolovať pred čistením", article["remember"], background="#f7fbf8", border="#dbe5de"))
    parts.append(callout("Najčastejšie chyby", article["mistakes"], background="#fff7f7", border="#eadada"))
    parts.append(f"<h2>{esc(article['expert_heading'])}</h2>")
    parts.extend(f"<p>{paragraph}</p>" for paragraph in article["expert"])
    parts.append(source_box(article))
    parts.append(commercial_blocks(article))
    parts.append(related_links(article["related"]))
    parts.append(faq(article))
    return "\n".join(parts)


DAMASK: dict[str, object] = {
    "title": "Čo je damask: obojstranný tkaný vzor a starostlivosť o obrusy a obliečky",
    "link": "co-je-damask-obojstranny-tkany-vzor-a-starostlivost-o-obrusy-a-obliecky",
    "meta": "Čo je damask, ako sa líši od brokátu a potlače a ako prať, odstraňovať škvrny, sušiť a žehliť damaskové obrusy a obliečky.",
    "short": "Damask je vzor vytvorený väzbou nití, nie názov jedného vlákna. Lesklé a matné plochy menia vzhľad podľa svetla a smeru, preto treba pri praní chrániť dlhšie väzné úseky, rozmery, farbu aj povrchovú úpravu.",
    "name": "damask",
    "locative": "damasku",
    "identity_heading": "Damask je tkaný vzor, nie druh vlákna",
    "identity_detail": "Damaskový motív vzniká striedaním väzobných efektov, často saténového alebo keprového charakteru, takže líc a rub ukazujú obrátený pomer lesklých a matných plôch.",
    "identity_boundary": "Bavlnený obrus, ľanový obrúsok, polyesterová obliečka a historický hodvábny damask môžu mať podobný motív, ale odlišnú reakciu na vodu, zásadu, trenie a žehlenie.",
    "label_focus": "bavlnu, ľan, hodváb alebo syntetiku, zmes, bordúru, výšivku, kovovú niť, podšívku, povrch proti škvrnám a povolenú teplotu žehlenia",
    "missing_label": "Pri bežnom modernom kuse možno vychádzať zo zloženia a skúšky skrytého lemu; pri staršom obruse, hodvábnej dekorácii alebo textílii s kovovou niťou zvoľte odborné posúdenie.",
    "dry_check": "vytiahnuté dlhšie nite, rozostúpenie pri šve, zoslabené sklady, vosk, zaschnuté jedlo, žlté línie na prehyboch a rozdielny lesk po predchádzajúcom žehlení",
    "damage_boundary": "Škvrnu možno ošetriť, no vytiahnutá niť sa drhnutím nevráti do väzby a lesklá stopa po príliš horúcej žehličke nie je zvyšok pracieho prostriedku.",
    "test_focus": "Na jednofarebnom damasku sledujte aj zmenu odrazu svetla: chemicky nepoškodená farba môže vyzerať fľakato, ak sa lokálne sploštila väzba.",
    "combined_risk": "napučania vlákna, pohybu dlhších väzných úsekov, ostrého preloženia a tlaku žehličky na reliéf",
    "chemistry_boundary": "Pri stole odlišujte mastnotu, bielkovinu, trieslovinové nápoje a vosk; jeden silný prostriedok nemusí byť vhodný na všetky štyri typy znečistenia.",
    "drying_detail": "Obrus rozložte tak, aby mokrá hmotnosť nevisela za jednu bordúru; obliečku otvorte v rohoch a viacvrstvový záves kontrolujte pri leme aj riasení.",
    "heat_boundary": "Príliš vysoké teplo môže zraziť celulózové vlákno, zmeniť dokončenie, vytvoriť lesklú plochu alebo poškodiť najcitlivejšiu zložku zmesi.",
    "stop_signs": "prenos farby, otváranie väzby, praskanie starej nite, lepkavá úprava, tmavnutie kovovej ozdoby alebo zväčšujúca sa vodná mapa",
    "professional_boundary": "Damaskový obrus na každodenné používanie možno často ošetrovať doma podľa etikety, zatiaľ čo historický hodváb, cirkevná textília alebo kombinácia s kovom vyžaduje konzervačný prístup.",
    "answer": "Damask je tkanina so vzorom vytvoreným väzbou, nie potlačou a nie jedným konkrétnym vláknom. Motív býva viditeľný z oboch strán v obrátenom lesklom a matnom účinku. Pri modernom bavlnenom alebo ľanovom obruse sa riaďte štítkom, škvrny riešte podľa ich typu, perte s voľným priestorom a kus vyberte hneď po cykle. Sušte ho vyrovnaný a žehlite z rubu pri povolenej teplote, ideálne ešte mierne vlhký. Dlhšie väzné úseky chráňte pred zipsami, kefou a drhnutím. Hodvábny, historický, kovom zdobený alebo iba profesionálne čistiteľný damask neperte podľa všeobecného návodu na obrusy.",
    "intro": "Hľadanie ako prať damaskový obrus často smeruje priamo k teplote, no najprv treba zistiť, čo slovo damask opisuje. Vzor môže byť utkaný z bavlny, ľanu, hodvábu, polyesteru aj zmesi a každé vlákno prináša inú hranicu. Navyše reliéf, bordúra, povrch odpudzujúci škvrny a dlhšie väzné úseky reagujú na trenie a tlak inak než obyčajné hladké plátno. Praktický postup preto spája identifikáciu, lokálne odstránenie škvrny, primeraný cyklus, rovnomerné sušenie a opatrné žehlenie.",
    "quick": [
        "<strong>Vzor je v konštrukcii:</strong> pri pravom damasku nevzniká iba farbou vytlačenou na povrchu.",
        "<strong>Zloženie určuje hranice:</strong> bavlna, ľan, hodváb a polyester sa neošetrujú jedným spoločným programom.",
        "<strong>Líc a rub sa opticky obracajú:</strong> rozdielny lesk sám osebe nie je vyblednutie.",
        "<strong>Dlhšie väzné úseky chráňte:</strong> zips, kefa a drsný uterák môžu vytiahnuť niť.",
        "<strong>Škvrnu nerozmazávajte:</strong> vosk, olej, víno a bielkoviny potrebujú odlišný prvý krok.",
        "<strong>Žehlite z rubu:</strong> silný tlak z líca môže sploštiť vzor a vytvoriť trvalý lesk.",
    ],
    "overview_heading": "Ako vzniká damaskový vzor a prečo sa mení so svetlom",
    "overview": [
        "V tkanej látke sa osnovné nite vedú pozdĺžne a útkové naprieč. Zmenou toho, ktoré nite prechádzajú nad a pod ostatnými, možno vytvoriť plochy s odlišným smerom väzných úsekov. Tie odrážajú svetlo rozdielne, preto sa ornament pri otočení javí svetlejší alebo tmavší. CottonWorks opisuje základné väzby a princíp väzných bodov; damask využíva tieto vzťahy na kresbu bez nutnosti ďalšej tlače.",
        "Slovo jacquard označuje mechanizmus a spôsob individuálneho riadenia osnovných nití pri zložitej vzorovanej tkanine. Damask je konkrétny typ výsledného vzorového efektu, ktorý sa môže na žakárovom stroji vyrábať. Preto nie je presné používať damask a jacquard vždy ako synonymá. Brokát môže mať viacfarebné alebo doplnkové vzorové nite a často pôsobí plastickejšie, kým jednofarebný damask stavia najmä na kontraste odrazu.",
        "Múzejné zbierky dokumentujú hodvábne damasky s technikami a materiálmi, ktoré sa nedajú preniesť na dnešný obrus z polyesteru. Zároveň ukazujú, prečo je pri staršom kuse dôležitá história, farbivo a stav priadze. Pevný vzhľad nevylučuje krehkosť: dlhé skladovanie na tom istom prehybe alebo svetlo môžu oslabiť jednu líniu skôr než zvyšok plochy.",
    ],
    "table1_heading": "Damask, brokát, žakárová tkanina a potlač",
    "table1_intro": "Pojmy sa v obchodoch prekrývajú, preto porovnávajte spôsob vzniku vzoru, zloženie a rub. Starostlivosť sa aj tak riadi celým výrobkom.",
    "table1_headers": ["Označenie", "Ako vzniká vzhľad", "Čo vidno na rube", "Praktická hranica"],
    "table1_rows": [
        ("Damask", "Vzor vytvára zmena väzobného efektu, často kontrast matu a lesku.", "Motív býva čitateľný v obrátenom svetelnom pomere.", "Chrániť dlhšie väzné úseky a žehliť bez sploštenia reliéfu."),
        ("Brokát", "Vzor môže tvoriť viac farieb a doplnkové nite, niekedy aj kovové.", "Rub môže mať voľnejšie alebo doplnkové priadze.", "Vyššie riziko zachytenia, migrácie farby a poškodenia ozdobnej nite."),
        ("Žakárová tkanina", "Širšia skupina zložitých vzorov vytvorených individuálnym riadením osnovy.", "Závisí od konkrétnej konštrukcie.", "Názov výrobnej techniky neurčuje vlákno ani povolené pranie."),
        ("Potlač s damaskovým motívom", "Ornament sa nanesie farbou na hotovú plochu.", "Rub môže byť svetlejší alebo bez motívu.", "Treba chrániť stálofarebnosť potlače; povrch nemá rovnakú väzobnú kresbu."),
    ],
    "sections": [
        {
            "heading": "Bavlnený, ľanový, hodvábny a syntetický damask",
            "paragraphs": [
                "Bavlnený damask býva savý a pri obruse znesie častejšiu údržbu, ak to povoľuje etiketa, no môže sa zraziť a silný lom sa po vysušení ťažšie vyrovnáva. Ľanový variant má výraznú krčivosť a po opakovanom používaní môže mäknúť. Zmes bavlny s polyesterom môže schnúť rýchlejšie, ale syntetická zložka znižuje bezpečnú teplotu žehlenia.",
                "Hodvábny damask posudzujte ako proteínový, často hodnotný textil. Pot, zásaditý prostriedok, vysoké teplo a svetlo môžu meniť pevnosť aj farbu. Viskózový alebo acetátový variant zas nemusí mať rovnakú mokrú stabilitu ako bavlna. Pri každom kuse má prednosť najcitlivejšie vlákno, farbivo a konštrukčný detail, nie historická povesť názvu damask.",
            ],
        },
        {
            "heading": "Ako prať damaskový obrus a obrúsky",
            "paragraphs": [
                "Obrus pred praním pretraste, odstráňte omrvinky a označte si škvrny, kým ich ešte vidíte. Vosk nechajte stuhnúť a opatrne nadvihnite, mastnotu odsajte a bielkovinovú škvrnu nezačínajte horúcou vodou. Bodový prostriedok skúšajte na skrytom leme. Veľký obrus nevkladajte do bubna stočený do pevnej gule, pretože vnútorné vrstvy sa horšie operú a opláchnu.",
                "Pri povolenom strojovom praní nechajte dostatok priestoru, zvoľte teplotu zo štítku a dávku podľa vody a náplne. Obrus nekombinujte so zipsami, uterákmi a predmetmi, ktoré môžu zachytiť väzbu. Po skončení ho vyberte bez odkladu, rozložte a zarovnajte okraje bez násilného naťahovania. Zaschnuté ostré záhyby sa vyrovnávajú ťažšie a silný tlak môže zmeniť lesk vzoru.",
            ],
        },
        {
            "heading": "Ako prať damaskové obliečky a posteľnú bielizeň",
            "paragraphs": [
                f"Obliečky obráťte podľa pokynov výrobcu, zapnite zips alebo gombíky a vytraste rohy. Plán starostlivosti dopĺňa návod <a href=\"{ARTICLE_BEDDING}\">ako správne prať obliečky</a>. Pri damasku navyše kontrolujte vytiahnuté nite a lesklé plochy, ktoré by sa mohli v preplnenom bubne viac trieť. Poťah s výšivkou alebo kontrastnou bordúrou perte iba po overení stálofarebnosti.",
                f"Hustota a počet nití nie sú samostatným pracím programom. Článok o <a href=\"{ARTICLE_THREAD_COUNT}\">počte nití pri obliečkach</a> vysvetľuje, prečo vyššie číslo automaticky nezaručuje väčšiu životnosť. Pri sušení otvorte rohy a preloženia, aby v nich nezostala vlhkosť. Frekvenciu prania prispôsobte používaniu, poteniu a zdravotnej situácii, nie iba dekoratívnemu vzhľadu.",
            ],
        },
        {
            "heading": "Víno, káva, čaj a ovocie na damasku",
            "paragraphs": [
                "Čerstvú tekutinu odsajte bielou savou tkaninou bez šúchania. Farbu nerozťahujte veľkým mokrým kruhom a neposypávajte ju náhodnou zmesou domácich surovín, ktorá môže zanechať ďalší zvyšok. Trieslovinová alebo pigmentová škvrna potrebuje produkt kompatibilný s vláknom a farbou. Najprv urobte skúšku na leme a dodržte čas z návodu.",
                f"Ak škvrna prešla cez obrus na podklad, oddeľte obe vrstvy, aby sa pri práci nevracala späť. Všeobecný postup pre rozličné druhy nečistôt nájdete v návode <a href=\"{ARTICLE_STAIN}\">ako odstraňovať škvrny z textilu</a>. Po ošetrení miesto dôkladne opláchnite povoleným spôsobom a pred žehlením overte, že farebný aj mastný zvyšok skutočne zmizol.",
            ],
        },
        {
            "heading": "Mastnota, omáčka a vosk zo sviečky",
            "paragraphs": [
                "Mastnotu najprv odsajte bez zatláčania do väzby. Na prateľnom farebne stálom kuse použite kompatibilný prostriedok v presnej koncentrácii; silné drhnutie môže vytvoriť matnú plochu viditeľnú pod bočným svetlom. Pri bielom obruse neznamená biela farba automatické povolenie chlóru, pretože zmes, niť bordúry alebo povrchová úprava môžu reagovať odlišne.",
                "Vosk nechajte vychladnúť a nadvihnite iba vrchnú stuhnutú časť tupou hranou. Horúca žehlička cez papier môže síce časť vosku roztaviť, ale zároveň ho zatlačiť hlbšie, preniesť farbivo zo sviečky a sploštiť damaskový efekt. Zvyšok riešte podľa etikety a kompatibility. Pred ďalším teplom musí byť mastná aj farebná zložka odstránená.",
            ],
            "callout": {
                "title": "Prvý krok podľa typu škvrny",
                "items": [
                    "Nápoj odsajte bez trenia a miesto udržte pod kontrolou.",
                    "Mastnotu najprv zachyťte savým materiálom, potom použite kompatibilný tenzid.",
                    "Bielkovinový zvyšok nezačínajte vysokou teplotou.",
                    "Vosk nechajte stuhnúť a nepretláčajte ho horúcou žehličkou cez vzor.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Zatrhnutá niť a rozostúpená väzba",
            "paragraphs": [
                "Dlhší väzný úsek sa môže zachytiť o prsteň, hranu stola alebo zips v práčke. Vytiahnutú slučku neodstrihujte, pretože môže pokračovať naprieč vzorom. Látku položte na rovnú plochu a bez ťahania rozložte napätie do okolia. Pri cennom obruse alebo viditeľnom strede zverte opravu človeku, ktorý pozná väzbu a vie niť vrátiť bez vytvorenia dierky.",
                "Rozostúpenie pri šve môže vzniknúť ťahom, oslabenou priadzou alebo príliš tesným stehom. Pranie otvor nezacelí. Miesto pred ďalším cyklom stabilizujte a skontrolujte aj protiľahlú stranu, pretože rovnaká záťaž mohla pôsobiť po celej línii. Pri novom výrobku stav odfoťte a riešte reklamáciu skôr, než niť skrátite alebo šev prešijete.",
            ],
        },
        {
            "heading": "Ako sušiť veľký obrus bez vytiahnutia okrajov",
            "paragraphs": [
                "Mokrý veľký obrus je výrazne ťažší než suchý. Úzka šnúra alebo dva štipce sústredia hmotnosť do malých bodov a môžu vytvoriť vytiahnutú líniu. Ak etiketa povoľuje sušenie zavesením, rozložte hmotnosť po šírke; pri citlivejšom kuse použite čistú rovnú plochu a prúdenie vzduchu. Okraje zarovnajte bez ťahania na pôvodný rozmer.",
                "Sušičku použite iba pri povolenom symbole. Mechanické prevaľovanie môže zvýrazniť trenie dlhších nití a presušenie vytvorí pevné záhyby. Ak obrus ostal v bubne po skončení, najprv ho rozložte a nechajte uvoľniť pri izbovej vlhkosti. Nesnažte sa každý lom odstrániť okamžite najvyššou teplotou žehličky.",
            ],
        },
        {
            "heading": "Ako žehliť damask a zachovať kresbu väzby",
            "paragraphs": [
                f"Žehlenie začnite na skrytom rohu a z rubu. Čistá ochranná tkanina znižuje priamy kontakt a mäkká podložka môže pomôcť, aby sa reliéf úplne nesploštil. Teplotu voľte podľa najcitlivejšieho vlákna a symbolu; ďalšie zásady rozoberá návod <a href=\"{ARTICLE_IRONING}\">ako správne žehliť textil</a>. Žehličku nenechávajte stáť na jednom ornamentálnom poli.",
                "Bavlnený alebo ľanový kus sa často vyrovnáva ľahšie mierne vlhký, no lokálne striekanie môže na nestálofarebnom alebo upravenom povrchu vytvoriť mapu. Najprv zvlhčite rovnomerne spôsobom povoleným výrobcom. Ak sa lesk po prechode mení, znížte tlak a teplotu. Syntetická zmes sa môže deformovať skôr, než sa prírodná zložka úplne vyrovná.",
            ],
        },
        {
            "heading": "Závesy, poťahy a damask, ktorý sa nedá vložiť do práčky",
            "paragraphs": [
                "Damaskový záves môže obsahovať podšívku, riasiacu pásku, závažie a rozdielne materiály na leme. Pred zvesením odstráňte prach vhodným nízkym saním cez ochrannú sieťku a odfoťte spôsob zavesenia. Ak výrobca povoľuje iba profesionálne čistenie, šírka metráže ani syntetické zloženie vrchnej vrstvy tento pokyn nemenia.",
                "Čalúnenie alebo pevný poťah má pod textíliou výplň a podklad, ktoré môžu zadržať vodu a preniesť farbu. Neaplikujte veľký objem roztoku bez overenia extrakcie a schnutia. Lokálny test robte na skrytom mieste vrátane úplného vyschnutia. Vodná mapa môže vzniknúť migráciou nečistoty z hĺbky, nie iba škvrnou na viditeľnom damasku.",
            ],
        },
        {
            "heading": "Skladovanie obrusov bez trvalého zlomu a žltnutia",
            "paragraphs": [
                "Čistý a úplne suchý obrus ukladajte voľne zrolovaný alebo preložený cez veľké oblúky podľa priestoru a hodnoty. Miesto prehybu pravidelne zmeňte, aby roky neležala celá záťaž na tej istej línii. Medzi vrstvy nepoužívajte farebný papier ani neoverený plast. Škvrna, ktorú pred uložením nevidno, môže oxidáciou zožltnúť.",
                "Skladovací priestor musí byť suchý, čistý a bez výrazných výkyvov. Prirodzené svetlo a vysoká teplota urýchľujú zmeny farby a vlákna. Pred sviatkom obrus vyberte s predstihom, prezrite pri bočnom svetle a mierne záhyby uvoľňujte postupne. Dlhodobo oslabený prehyb nevyrovnávajte prudkým ťahom.",
            ],
        },
        {
            "heading": "Ako vybrať damaskový obrus alebo obliečky",
            "paragraphs": [
                "Pri kúpe si pozrite zloženie, hmotnosť, rub vzoru, kvalitu okraja a ošetrovacie symboly. Na každodenný obrus je dôležitá reálna prateľnosť a jednoduchosť odstránenia škvŕn, nie iba bohatý lesk pod osvetlením predajne. Pri obliečkach sledujte dotyk, priedušnosť, rozmer po predpokladanom zrazení a konštrukciu zapínania.",
                "Povrch odpudzujúci škvrny môže zmeniť savosť a neskorší postup čistenia; pýtajte sa na jeho údržbu a životnosť. Reštauračný obrus potrebuje iný cyklus než príležitostná rodinná textília. Kvalitu nemožno odvodiť iba zo slova hotelový alebo luxusný. Lepšia je jasná etiketa, pevný rovný lem a materiál vhodný na zamýšľanú frekvenciu používania.",
            ],
        },
    ],
    "table2_heading": "Damask po praní: ako čítať zmenu povrchu",
    "table2_intro": "Výsledok posudzujte po úplnom vysušení pri rovnakom smere svetla. Optická zmena nie je vždy strata farby, ale mechanické poškodenie nemožno vyprať späť.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Vzor pôsobí plocho a príliš lesklo", "Silný tlak, vysoké teplo alebo zmena povrchovej úpravy.", "Porovnať líc, rub a skrytý lem pod bočným svetlom.", "Znížiť teplotu a tlak; trvalú zmenu nedrhnúť."),
        ("Pri šve je svetlá medzera", "Posun nití, ťah alebo oslabený steh.", "Celistvosť priadze a smer napätia.", "Pred ďalším praním stabilizovať a opraviť."),
        ("Obrus je menší alebo má zvlnený lem", "Zrazenie vlákna, rozdielna stabilita bordúry alebo vysoké teplo.", "Mieru po úplnom vysušení a zloženie okraja.", "Nenapínať silou; pri novom kuse zdokumentovať zmenu."),
        ("Ostal klzký alebo tuhý povrch", "Nadbytok produktu, slabý oplach alebo zmena dokončenia.", "Dávku, náplň, tvrdosť vody a skrytú skúšku.", "Pri povolení zvoliť šetrný oplach bez ďalšej chémie."),
        ("Na prehybe je žltá alebo biela línia", "Stará škvrna, oxidácia, svetlo alebo mechanicky oslabené vlákno.", "Pevnosť nite a históriu skladovania.", "Neohýbať naspäť silou; pri hodnotnom kuse odborné posúdenie."),
    ],
    "steps_heading": "Ako ošetriť prateľný damask krok za krokom",
    "steps": [
        "Overte vlákna, bordúru, ozdoby, povrchovú úpravu a všetky symboly ošetrovania.",
        "Obrus alebo obliečku pretraste, prezrite proti svetlu a jednotlivé škvrny roztrieďte podľa typu.",
        "Lokálny prostriedok skúste na skrytom leme a výsledok vyhodnoťte až po vyschnutí.",
        "Oddeľte damask od zipsov, suchých zipsov, drsných uterákov a nestálofarebných kusov.",
        "Použite iba povolený cyklus, primeranú dávku a dostatok priestoru na pohyb a oplach.",
        "Po skončení kus hneď vyberte, rozložte a bez ťahania zarovnajte okraje a švy.",
        "Sušte s rovnomernou oporou a zabezpečte prúdenie vzduchu aj v rohoch a preloženiach.",
        "Žehlite z rubu cez ochrannú tkaninu pri najnižšej účinnej povolenej teplote.",
    ],
    "remember": [
        "Je vzor skutočne tkaný a aké vlákna tvoria celý výrobok?",
        "Má kus hodváb, kovovú niť, výšivku, bordúru alebo povrch proti škvrnám?",
        "Sú dlhšie nite, okraje a švy bez zatrhnutia a rozostúpenia?",
        "Poznáte typ každej škvrny a kompatibilitu lokálneho produktu?",
        "Má veľký mokrý kus počas sušenia dostatočnú oporu?",
        "Je povolená para, sušička a konkrétna teplota žehlenia?",
    ],
    "mistakes": [
        "Považovať damask za jedno vlákno a zvoliť program iba podľa názvu vzoru.",
        "Prať veľký obrus stočený v preplnenom bubne spolu so zipsami a uterákmi.",
        "Drhnúť víno alebo mastnotu a vytvoriť matnú plochu v lesklom vzore.",
        "Pretláčať vosk horúcou žehličkou hlbšie do väzby.",
        "Zavesiť mokrý obrus za dva body a vytiahnuť bordúru.",
        "Žehliť vysokým tlakom z líca a sploštiť damaskový efekt.",
    ],
    "expert_heading": "Odbornejší pohľad: väzné úseky, odraz svetla a meranie zmien",
    "expert": [
        "Väzba určuje rozloženie väzných bodov a dĺžku úsekov priadze na povrchu. Dlhší úsek môže odrážať viac svetla a vytvoriť hladký efekt, ale zároveň má väčšiu plochu dostupnú na zachytenie. Zmena lesku po praní preto môže súvisieť s posunom priadze, fibriláciou, zvyškom produktu alebo sploštením pri žehlení. Bez porovnania rubu, smeru svetla a skrytého miesta nemožno príčinu určiť iba fotografiou.",
        "AATCC TM135 opisuje kontrolované určovanie rozmerovej zmeny po domácom praní a AATCC TM61 zrýchlené hodnotenie stálofarebnosti pri praní. Metódy definujú zariadenie, podmienky a hodnotenie; nie sú prísľubom, že každý damask znesie rovnaký cyklus. Pre domácnosť je užitočné zmerať nový obrus medzi rovnakými referenčnými bodmi a zapisovať iba výsledok po úplnom vysušení bez naťahovania.",
        "Pri historickom textile je čistenie nevratný zásah. Múzejná konzervácia skúma vlákna, farbivá, výzdobu, predchádzajúce opravy a riziko straty materiálu. To je odlišný cieľ než odstránenie škvrny z moderného kuchynského obrusu. Čím menej poznáte pôvod, tým dôležitejšie je obmedziť experiment a zachovať dokumentáciu o stave.",
    ],
    "source_intro": "Zdroje vysvetľujú väzby, vláknové označovanie, skúšanie rozmerov a farby a múzejný kontext damasku. Neurčujú spoločný prací program pre všetky výrobky.",
    "sources": [
        ("CottonWorks: Weaving 101", COTTONWORKS_WEAVING),
        ("Nariadenie EÚ 1007/2011 o názvoch textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: systém symbolov ošetrovania", GINETEX),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("Victoria and Albert Museum: reverzibilný damaskový vzor", V_AND_A_DAMASK),
    ],
    "product_heading": "Prací prostriedok pre prateľný damask vyberajte podľa zloženia",
    "product_intro": "Pri modernom obruse alebo obliečkach, ktoré výrobca povoľuje prať, má zmysel presné dávkovanie gélu podľa tvrdosti vody, veľkosti a znečistenia náplne.",
    "product_name": LAUNDRY_PRODUCT_NAME,
    "product_url": LAUNDRY_PRODUCT_URL,
    "product_text": "Tekutý prací prostriedok sa dá presne dávkovať a pri lokálnom predčistení použiť iba spôsobom uvedeným na jeho obale. Pred celým cyklom overte stálofarebnosť a symboly damaskového výrobku.",
    "product_limit": "Produkt použite len na textíliu, ktorej zloženie a etiketa ho povoľujú. Nenahrádza profesionálne čistenie hodvábneho, historického, kovom zdobeného ani lepeného kusu.",
    "category_heading": "Porovnajte pracie gély podľa náplne a materiálu",
    "category_intro": "Pri bežnej prateľnej stolovej a posteľnej bielizni porovnávajte určenie produktu, dávkovanie a kompatibilitu s farbou. Názov damask neurčuje jednu správnu receptúru.",
    "category_name": LAUNDRY_CATEGORY_NAME,
    "category_url": LAUNDRY_CATEGORY_URL,
    "category_text": "V kategórii nájdete pracie gély pre rôzne typy bielizne. Pred výberom skontrolujte vlákno, farbu, škvrnu a najnižší limit celého výrobku.",
    "related": [
        ("Ako správne prať obliečky", ARTICLE_BEDDING),
        ("Ako často prať posteľné prádlo", ARTICLE_BEDDING_FREQ),
        ("Čo znamená počet nití pri obliečkach", ARTICLE_THREAD_COUNT),
        ("Ako odstrániť rôzne škvrny z textilu", ARTICLE_STAIN),
        ("Prečo farby pri praní a trení blednú", ARTICLE_COLOR),
        ("Ako správne vyžehliť textil", ARTICLE_IRONING),
    ],
    "faq_title": "damask, damaskové obrusy a obliečky",
    "faq": [
        ("Je damask vždy hodvábny?", "Nie. Damask opisuje tkaný vzorový efekt a môže byť z hodvábu, bavlny, ľanu, polyesteru alebo zmesi."),
        ("Ako spoznám damask od potlače?", "Pri tkanom damasku tvorí motív väzba a na rube sa často javí v obrátenom pomere lesklých a matných plôch. Potlač môže mať rub bez motívu."),
        ("Na koľko stupňov sa perie damaskový obrus?", "Neexistuje spoločná teplota. Rozhoduje vláknové zloženie, farba, úprava, bordúra a symbol na konkrétnom obruse."),
        ("Môžem na biely damask použiť bielidlo?", "Iba ak ho povoľuje symbol a zloženie. Biela farba sama osebe nepotvrdzuje odolnosť voči chlóru."),
        ("Ako odstránim vosk z damasku?", "Nechajte ho stuhnúť, opatrne nadvihnite vrchnú časť a zvyšok riešte podľa vlákna a etikety. Horúce prežehlenie môže vosk aj farbivo zatlačiť hlbšie."),
        ("Prečo je vzor po žehlení príliš lesklý?", "Silný tlak alebo vysoké teplo mohli sploštiť väzbu či zmeniť povrch. Ďalej ho nedrhnite; porovnajte rub a skrytý lem."),
        ("Môže ísť damask do sušičky?", "Len pri povolenom symbole. Pri zmesi, hodvábe, úprave alebo ozdobnej bordúre môže byť limit nižší."),
        ("Ako skladovať sviatočný damaskový obrus?", "Úplne čistý a suchý, bez tesného plastu, s veľkými prekladmi alebo voľne zrolovaný. Miesto prehybu občas zmeňte."),
        ("Je jacquard to isté ako damask?", "Nie úplne. Jacquard označuje širší výrobný systém zložitých vzorov; damask je konkrétny druh väzobného a svetelného efektu."),
    ],
}


KAPOK: dict[str, object] = {
    "title": "Čo je kapok: ľahká rastlinná výplň, vlhkosť a starostlivosť",
    "link": "co-je-kapok-lahka-rastlinna-vypln-vlhkost-a-starostlivost",
    "meta": "Čo je kapok, ako sa líši od peria a polyesterovej výplne a ako ošetrovať kapokový vankúš, podsedák a poťah bez zlepenia výplne.",
    "short": "Kapok je jemné plodové vlákno z vnútra plodov stromu Ceiba pentandra. Má dutú štruktúru a voskovitý povrch, no hotový vankúš môže zadržať vlhkosť v hustej náplni. Prateľnosť preto určuje celý výrobok, nie povesť samotného vlákna.",
    "name": "kapok",
    "locative": "kapoku",
    "identity_heading": "Kapok je plodové vlákno, nie bavlna ani perie",
    "identity_detail": "Európske pravidlá opisujú kapok ako vlákno získané z vnútra plodov stromu Ceiba pentandra. Jemné bunky majú veľký vnútorný priestor a povrchové vosky, čo pomáha vysvetliť nízku objemovú hmotnosť aj odlišné zmáčanie.",
    "identity_boundary": "Vo vankúši však môže byť čistý voľný kapok, zmes s iným vláknom, prešívaná rohož, ochranná vnútorná obliečka a samostatný snímateľný poťah. Každá vrstva potrebuje vlastné posúdenie.",
    "label_focus": "podiel kapoku a ďalších výplní, materiál vnútorného vaku, snímateľnosť poťahu, zips, komory, prešívanie, povolenie ponorenia a presný spôsob sušenia jadra",
    "missing_label": "Ak neviete, či je výplň voľná alebo stabilizovaná a výrobca nepotvrdil pranie jadra, bezpečnejšie je vyčistiť iba snímateľný poťah a samotnú náplň chrániť pred zmáčaním.",
    "dry_check": "únik vlákien cez šev, zlepené zhluky, prázdne komory, škvrnu na vnútornom vaku, stopy plesne, starý pach a poškodený zips alebo prešívanie",
    "damage_boundary": "Zhlukovanie a presun výplne sú mechanické alebo vlhkostné zmeny; ďalšia dávka prostriedku ich nemusí opraviť a môže predĺžiť schnutie.",
    "test_focus": "Pri výplni nestačí sledovať poťah. Overte aj to, či vlhkosť preniká k jadru a či sa skúšobná zóna po vyschnutí vráti k pôvodnému objemu bez tvrdého okraja.",
    "combined_risk": "nerovnomerného zmáčania voskovitého povrchu, stlačenia jemných dutých vlákien a pomalého odvodu vody z hustého jadra",
    "chemistry_boundary": "Aviváž, olej, silná parfumácia a nadbytok gélu môžu obaliť výplň alebo zostať v hĺbke. Produkt určený na poťah nemožno automaticky aplikovať na voľné kapokové jadro.",
    "drying_detail": "Vankúš alebo podsedák treba pri povolenom mokrom čistení sušiť cez celú hrúbku, pravidelne meniť polohu a kontrolovať stred, komory, švy aj priestor pri zipse.",
    "heat_boundary": "Prudké teplo môže zraziť obal, poškodiť šev, meniť voskovité zložky povrchu a vysušiť obvod skôr, než sa odparí voda zo stredu.",
    "stop_signs": "zatuchnutý alebo plesnivý pach, viditeľná pleseň, trvalo studený stred, hnedá migrujúca mapa, rozchádzajúci sa šev alebo výplň unikajúca cez tkaninu",
    "professional_boundary": "Pri malej lokálnej škvrne môže stačiť povrchové ošetrenie obalu, no veľké premočenie, pleseň, biologická kontaminácia alebo neznámy starší vankúš vyžadujú odborné hygienické a materiálové posúdenie.",
    "answer": "Kapok je veľmi ľahké rastlinné vlákno z vnútra plodov stromu Ceiba pentandra. Dutá bunka a voskovitý povrch pomáhajú vláknu odpudzovať vodu na povrchu, ale hustá výplň vankúša môže po premočení schnúť dlho a nerovnomerne. Najprv preto oddeľte prateľný snímateľný poťah od jadra. Poťah perte podľa vlastnej etikety. Celý kapokový vankúš alebo podsedák namáčajte iba vtedy, keď to výslovne povoľuje výrobca a máte podmienky na úplné vysušenie celej hrúbky. Výplň nekrúťte, nestláčajte silou a neobaľujte avivážou či olejom. Pach neprekrývajte vôňou; hľadajte vlhkosť, znečistený obal alebo poškodené jadro.",
    "intro": "Otázka ako prať kapokový vankúš znie jednoducho, ale môže označovať tri rozdielne úlohy: vyprať odnímateľnú obliečku, očistiť vnútorný vak alebo premočiť samotnú výplň. Posledný krok je najrizikovejší. Kapok síce prijíma kvapku vody inak než bavlna, no v hustom výrobku sa voda môže dostať medzi vlákna, do švov a k inej prímesi. Ak jadro nevyschne rovnomerne, vznikajú zhluky, strata objemu a pach. Bezpečná starostlivosť preto začína rozobratím konštrukcie a presným návodom výrobcu.",
    "quick": [
        "<strong>Kapok pochádza z plodu:</strong> nejde o perie, vlnu ani bavlnu zo semenných chĺpkov.",
        "<strong>Duté vlákno je veľmi ľahké:</strong> nízka hmotnosť však neznamená, že celý vankúš rýchlo vyschne.",
        "<strong>Poťah a jadro sú dve úlohy:</strong> prateľná obliečka nedokazuje prateľnosť výplne.",
        "<strong>Voda sa môže rozložiť nerovnomerne:</strong> voskovitý povrch a hustota náplne podporujú lokálne zhluky.",
        "<strong>Stred musí byť úplne suchý:</strong> suchý povrch vankúša nestačí.",
        "<strong>Pleseň nie je parfumový problém:</strong> pri viditeľnom raste alebo trvalom pachu zásah zastavte.",
    ],
    "overview_heading": "Čo je kapok a prečo je jeho vlákno také ľahké",
    "overview": [
        "Kapokové vlákna vystielajú vnútro toboliek tropického stromu Ceiba pentandra a pomáhajú pri šírení semien. Nariadenie EÚ ich uvádza ako samostatný názov textilného vlákna. Na rozdiel od ľanu alebo ramie nejde o lýko zo stonky. Na rozdiel od bavlny sa kapok pre hladký povrch a mechanické vlastnosti spriada ťažšie, preto sa často používa ako voľná výplň alebo v zmesiach.",
        "Recenzované materiálové práce opisujú veľký dutý kanál, tenkú bunkovú stenu a voskovité látky na povrchu. Tieto znaky súvisia s nízkou hustotou, vztlakom a oleofilným správaním skúmaným pri sorpčných materiáloch. Laboratórny výsledok pre upravené vlákno však nie je domácim návodom na pranie hotového vankúša. Ten obsahuje masu stlačených vlákien, obal a švy, cez ktoré musí vlhkosť odísť.",
        "Nový kapok býva pružne nadýchaný, ale pri používaní sa presúva a stláča. Pot, vodná para a rozliata tekutina vstupujú najprv cez poťah. Ak je vnútorný vak hustý, môže navonok vyzerať čistý, zatiaľ čo jedna komora ostáva vlhká. Hygienu preto nezabezpečuje iba názov prírodnej výplne, ale bariéra poťahu, pravidelné vetranie, kontrola švov a riešenie nehody bez odkladu.",
    ],
    "table1_heading": "Kapok, perie, polyester, vlna a bavlnená výplň",
    "table1_intro": "Porovnanie zachytáva typické rozdiely vo výplniach. Konkrétny výrobok môže byť zmesový, prešívaný alebo mať vlastný servisný postup.",
    "table1_headers": ["Výplň", "Štruktúra", "Správanie po navlhnutí", "Čo rozhoduje o starostlivosti"],
    "table1_rows": [
        ("Kapok", "Jemné duté plodové vlákna s voskovitým povrchom.", "Môže sa zmáčať nerovnomerne, zhlukovať a držať vodu medzi vláknami.", "Návod jadra, hustota náplne, komory a možnosť úplného sušenia."),
        ("Perie a páperie", "Rozvetvená živočíšna štruktúra s vysokým objemom.", "Zlepenie a strata objemu pri nevhodnom praní alebo nedosušení.", "Druh náplne, tesnosť vaku, povolený cyklus a dlhé rovnomerné sušenie."),
        ("Polyesterové rúno alebo guličky", "Tvarované syntetické vlákna v rúne alebo voľnej náplni.", "Podľa konštrukcie sa môže zľahnúť, zhlukovať alebo deformovať teplom.", "Tvar vlákna, prešívanie, teplotný limit a pokyn výrobcu."),
        ("Vlna", "Kučeravé proteínové vlákna v rúne alebo guľôčkach.", "Pohyb, teplo a zmena pH môžu podporiť plstnatenie.", "Povolenie prania, úprava vlny a šetrná mechanika."),
        ("Bavlna", "Savé celulózové vlákna v rúne alebo hutnej náplni.", "Prijíma viac vody, je ťažká a môže dlho schnúť.", "Hrúbka, zrážanie obalu a reálna ventilácia jadra."),
    ],
    "sections": [
        {
            "heading": "Voľný kapok, zmesová výplň a prešívané rúno",
            "paragraphs": [
                "Voľná výplň sa môže cez zips dopĺňať alebo uberať a pri používaní sa presúva medzi zónami. Zmes s bavlnou, latexovou drvinou, polyesterom alebo iným vláknom už nereaguje ako čistý kapok. Prešívané rúno drží tvar väzbou alebo spojivom, ktoré môže mať nižší limit vody a tepla než samotné rastlinné vlákno.",
                "Marketingové pomenovanie prírodný vankúš nehovorí, či je výplň čistá, či možno prať jadro alebo či je iba poťah z prírodného materiálu. Hľadajte percentá, návod na údržbu každej oddeliteľnej časti a informáciu o doplnení náplne. Ak výrobca poskytuje iba pokyn na vetranie a čistenie poťahu, nevytvárajte si povolenie na ponorenie jadra.",
            ],
        },
        {
            "heading": "Kapokový vankúš, meditačný podsedák a matracová vrstva",
            "paragraphs": [
                "Spací vankúš prijíma pot, kožný maz a vodnú paru každú noc. Meditačný podsedák znáša tlak v jednej zóne a môže ležať priamo na podlahe. Tenká matracová vrstva má veľkú plochu, ale vnútro sa môže vetrať nerovnomerne. Rovnaká výplň teda čelí odlišnej kombinácii vlhkosti, kompresie a nečistôt.",
                "Vankúš pravidelne otáčajte a jemne preusporiadajte výplň spôsobom povoleným výrobcom. Podsedák nenechávajte na vlhkej podlahe a po použití ho vetrajte z oboch strán. Matracovú vrstvu neprekladajte do ostrého lomu, ak to konštrukcia neumožňuje. Cieľom je obmedziť trvalé stlačenie a umožniť vodnej pare odísť skôr, než vznikne pach.",
            ],
        },
        {
            "heading": "Prečo hydrofóbny povrch neznamená vodotesný vankúš",
            "paragraphs": [
                "Jednotlivé kapokové vlákno môže na povrchu vodu odpudzovať, no medzi tisíckami vlákien sú priestory, do ktorých sa tekutina dostane tlakom, časom a pomocou tenzidu. Obal a šev navyše môžu byť savé. Keď sa voda rozloží nerovnomerne, jedna časť výplne ostane ľahká a druhá sa zlepí do vlhkej hrudky.",
                "Prací prostriedok znižuje povrchové napätie, takže správanie čistej kvapky vody nemožno preniesť na prací roztok. Žmýkanie síce vytlačí časť vody, ale môže trvalo stlačiť alebo presunúť náplň. Preto sa pri povolenom mokrom čistení postupuje podľa presného návodu vrátane veľkosti zariadenia, podpory pri prenášaní a spôsobu sušenia.",
            ],
        },
        {
            "heading": "Ako prať snímateľný poťah bez namočenia jadra",
            "paragraphs": [
                f"Poťah opatrne odopnite tak, aby kapok neunikal cez vnútorný zips. Pred praním zatvorte zapínanie a riaďte sa jeho vlastnou etiketou. Všeobecný postup dopĺňa článok <a href=\"{ARTICLE_BEDDING}\">ako prať posteľné obliečky</a>. Škvrnu na poťahu ošetrujte od rubu, ak to konštrukcia povoľuje, a mokrý produkt neprenášajte späť na výplň.",
                "Poťah nasaďte až po úplnom vysušení. Aj mierne vlhká tkanina môže uzavrieť vodu pri jadre a vytvoriť chladnú zónu. Vnútorný vak medzitým chráňte pred prachom na suchej priedušnej ploche. Ak má poťah zips, šnúrku, výšivku alebo protišmykovú vrstvu, bezpečnú teplotu určuje tento celý komponent.",
            ],
        },
        {
            "heading": "Môže sa prať celý kapokový vankúš",
            "paragraphs": [
                "Iba vtedy, keď výrobca konkrétne povoľuje mokré čistenie jadra a uvádza postup. Potrebujete zariadenie alebo nádobu, v ktorej sa vankúš nestlačí do pevnej gule, a reálnu možnosť vysušiť stred. Pokyn na pranie obliečky alebo univerzálny symbol na obale nie je dostatočný, ak nie je jasné, na ktorú časť sa vzťahuje.",
                "Pri povolenom cykle nepoužívajte väčšiu dávku v nádeji, že prenikne hlbšie. Nadbytok sa horšie oplachuje a zostáva v náplni. Mokré jadro podoprite po celej ploche, nezdvíhajte za jeden šev a neskrúcajte. Poškodený alebo starý vnútorný vak môže počas pohybu prasknúť a uvoľniť vlákna do zariadenia.",
            ],
            "callout": {
                "title": "Pred ponorením kapokovej výplne musíte poznať",
                "items": [
                    "Či výrobca povoľuje čistenie jadra, nielen snímateľného poťahu.",
                    "Či je výplň čistý kapok, zmes, rúno alebo voľná náplň v komorách.",
                    "Ako sa má mokrý výrobok preniesť, vytvarovať a vysušiť cez celú hrúbku.",
                    "Čo urobíte, ak stred ostane po odporúčanom čase vlhký alebo zhluknutý.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Rozliata voda, pot, moč a biologické znečistenie",
            "paragraphs": [
                "Čerstvú tekutinu odsajte čistými savými uterákmi z oboch dostupných strán bez prudkého stláčania do stredu. Odstráňte poťah a zistite, či je vnútorný vak mokrý. Malá povrchová nehoda sa môže skončiť lokálnym ošetrením poťahu; veľké premočenie jadra je iná situácia a vyžaduje postup výrobcu alebo odborné posúdenie.",
                "Moč, krv, zvratky alebo povodňová voda prinášajú hygienické riziko, ktoré samotné vysušenie nerieši. Dezinfekčný prostriedok zároveň nemusí byť kompatibilný s vláknom a nemusí sa dať z hĺbky vypláchnuť. Pri rozsiahlej kontaminácii, zraniteľnom používateľovi alebo nejasnom preniknutí do jadra zvážte výmenu výplne namiesto domáceho parfumovania.",
            ],
        },
        {
            "heading": "Zhluky, strata objemu a presun kapoku do rohov",
            "paragraphs": [
                "Pri pravidelnom tlaku sa výplň presúva a hutnie. Jemné premasírovanie suchého vankúša môže uvoľniť zóny, ak to dovoľuje obal, ale násilné trhanie cez tkaninu môže poškodiť vlákna a šev. Výplň so servisným zipsom upravujte na čistej ploche bez prievanu a podľa návodu, aby jemné vlákna neunikli do miestnosti.",
                "Zhluk po navlhnutí najprv úplne vysušte kontrolovaným prúdením vzduchu. Až potom hodnotíte, či sa dá objem obnoviť. Tvrdá, zapáchajúca alebo farebne zmenená hrudka môže obsahovať zvyškovú vlhkosť alebo kontamináciu. Neuzatvárajte ju do nového poťahu a nepridávajte čerstvý kapok k neoverenému vlhkému jadru.",
            ],
        },
        {
            "heading": "Ako sušiť kapokový vankúš, ak výrobca povoľuje pranie",
            "paragraphs": [
                "Vankúš položte na čistú priedušnú oporu a pravidelne meňte polohu. Prúdenie vzduchu musí dosiahnuť hornú aj spodnú stranu. Hrubé komory jemne kontrolujte rukou bez silného stlačenia. Povrchový poťah môže byť suchý v priebehu hodín, kým jadro potrebuje podstatne dlhšie podľa hrúbky, vlhkosti vzduchu a množstva zadržanej vody.",
                f"Rady pre sušenie v malom priestore rozoberá článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>, pri výplni však pridajte kontrolu stredu. Neurýchľujte proces horúcim fénom ani radiátorom. Ak výrobca povoľuje sušičku, dodržte presné nastavenie a kapacitu; povolenie nemožno odvodiť z toho, že poťah je bavlnený.",
            ],
        },
        {
            "heading": "Pach kapoku, zatuchnutie a podozrenie na pleseň",
            "paragraphs": [
                f"Prírodný materiál môže mať mierny vlastný pach, ale nový silný zatuchnutý tón po navlhnutí signalizuje potrebu kontroly. Prezrite švy, vnútorný vak, spodnú stranu a miesto, kde vankúš ležal. Všeobecné príčiny rozoberá článok <a href=\"{ARTICLE_ODOR}\">prečo textil zapácha aj po praní</a>. Vôňa príčinu neodstráni a môže sťažiť hodnotenie.",
                "Viditeľné bodky, rozrastajúca sa mapa alebo pach, ktorý sa vracia po zahriatí, nečistite naslepo ďalším mokrým cyklom. Spóry a produkty rastu môžu byť v hĺbke, kam povrchový zásah nedosiahne. Pri alergikovi, dieťati alebo rozsiahlej ploche zvoľte odborné posúdenie a podľa stavu výmenu. Zdravotné rozhodnutie má prednosť pred zachovaním výplne.",
            ],
        },
        {
            "heading": "Vetranie, slnko a skladovanie kapokovej výplne",
            "paragraphs": [
                "Vankúš vetrajte v suchom priestore a pravidelne kontrolujte podklad. Krátke rozptýlené svetlo a vzduch nie sú to isté ako dlhé pečenie na prudkom slnku, ktoré môže meniť farbu obalu a starnutie materiálu. Výrobok neopierajte o vlhkú stenu a neukladajte ho priamo na nevetranú podlahu.",
                "Na dlhšie uloženie použite čistý priedušný obal a nestláčajte výplň do minimálneho vákuového objemu, ak to výrobca neodporúča. Pred uložením musí byť jadro úplne suché. Po vybratí skontrolujte pach, zhluky a stav švov skôr, než výrobok použijete. Skladovací parfum nie je náhradou kontroly vlhkosti.",
            ],
        },
        {
            "heading": "Ako vybrať kapokový vankúš a čitateľný návod",
            "paragraphs": [
                "Pri kúpe hľadajte presné zloženie výplne, hmotnosť, snímateľný poťah, hustotu vnútorného vaku a možnosť doplnenia alebo odobratia kapoku. Praktickou výhodou je samostatne prateľný poťah a jasne oddelený návod pre jadro. Všeobecné slová ekologický, antibakteriálny alebo samoočistný nenahrádzajú skúšobné údaje ani hygienický režim.",
                "Posúďte tiež zamýšľané použitie. Spací vankúš, dekoratívny vankúš a podsedák potrebujú inú oporu a znášajú inú frekvenciu vlhkosti. Ak predajca nevie vysvetliť, čo robiť pri premočení, je to dôležitá informácia pre rozhodnutie. Opraviteľný zips a dostupná náhradná náplň môžu predĺžiť životnosť viac než výrazné tvrdenie bez hraníc.",
            ],
        },
    ],
    "table2_heading": "Kapoková výplň po používaní alebo navlhnutí",
    "table2_intro": "Najprv oddeľte bežné stlačenie od zvyškovej vlhkosti a hygienického problému. Nehodnoťte jadro iba podľa suchého poťahu.",
    "table2_headers": ["Prejav", "Možná príčina", "Kontrola", "Ďalší krok"],
    "table2_rows": [
        ("Výplň je v rohoch a stred je prázdny", "Presun voľného kapoku tlakom a pohybom.", "Suchosť, švy, zips a rovnomernosť náplne.", "Suchý kus jemne preusporiadať podľa návodu alebo doplniť servisným otvorom."),
        ("Vo vnútri sú tvrdé hrudky", "Nerovnomerné navlhnutie, zvyšok produktu alebo dlhé stlačenie.", "Pach, teplotu stredu a farebné mapy.", "Najprv úplne vysušiť; kontaminovanú výplň nemiešať s novou."),
        ("Stred je chladný aj po suchom povrchu", "Zvyšková vlhkosť v hustej náplni.", "Viac miest, komory a spodnú stranu.", "Pokračovať v bezpečnom prúdení vzduchu, nepoužívať ani neukladať."),
        ("Z poťahu unikajú jemné vlákna", "Riedky alebo poškodený vnútorný vak, otvorený šev.", "Líniu stehu a rozsah úniku.", "Pred manipuláciou opraviť alebo vymeniť obal na čistej ploche."),
        ("Pach sa vracia po zahriatí", "Vlhkosť, biologické znečistenie alebo degradovaná časť jadra.", "Viditeľnú pleseň, históriu premočenia a vnútorný vak.", "Neparfumovať; pri podozrení na pleseň odborné posúdenie alebo výmena."),
    ],
    "steps_heading": "Ako sa starať o kapokový vankúš krok za krokom",
    "steps": [
        "Zistite presné zloženie výplne a oddeľte návod snímateľného poťahu od pokynov pre jadro.",
        "Prezrite švy, zips, vnútorný vak, zhluky, pach a stopy vlhkosti na oboch stranách.",
        "Snímateľný poťah perte samostatne podľa jeho etikety a na jadro ho vráťte až úplne suchý.",
        "Malú nehodu okamžite odsajte bez zatláčania tekutiny hlbšie do výplne.",
        "Celé jadro ponorte iba pri výslovnom povolení výrobcu a s pripraveným spôsobom úplného sušenia.",
        "Mokrý výrobok prenášajte s celoplošnou oporou, nekrúťte ho a neťahajte za jediný šev.",
        "Počas sušenia pravidelne meňte polohu a kontrolujte stred, komory, švy aj spodnú stranu.",
        "Úplne suchú výplň uložte voľne v čistom priedušnom obale bez dlhého silného stlačenia.",
    ],
    "remember": [
        "Je vnútri čistý kapok, zmes, voľná náplň alebo prešívané rúno?",
        "Vzťahuje sa symbol prania na poťah, vnútorný vak alebo celý výrobok?",
        "Dokážete po namočení overiť a vysušiť stred celej hrúbky?",
        "Je zips, šev a tkanina vnútorného vaku dostatočne pevná?",
        "Ide o bežnú vlhkosť, lokálnu škvrnu alebo biologickú kontamináciu?",
        "Má výrobca postup na dopĺňanie a preusporiadanie výplne?",
    ],
    "mistakes": [
        "Predpokladať, že vodoodpudivý povrch jednotlivého vlákna robí celý vankúš vodotesný.",
        "Vyprať jadro podľa etikety snímateľného bavlneného poťahu.",
        "Použiť priveľa gélu, aviváž alebo olej a predĺžiť oplach a schnutie.",
        "Žmýkať výplň krútením alebo mokrý vankúš zdvihnúť za jeden roh.",
        "Posúdiť suchosť iba dotykom vrchného poťahu.",
        "Prekryť zatuchnutie vôňou a uložiť kapok späť do uzavretého obalu.",
    ],
    "expert_heading": "Odbornejší pohľad: dutina vlákna, vosky a pohyb vody v náplni",
    "expert": [
        "Kapokové vlákno má veľký lumen a tenkú bunkovú stenu. Recenzované práce opisujú aj povrchové vosky, ktoré prispievajú k hydrofóbnemu a oleofilnému správaniu. Výskum často sleduje sorpciu oleja, kompozity alebo chemicky upravené vlákna. Také výsledky pomáhajú chápať štruktúru, ale nevypovedajú priamo o hygienickom praní spotrebiteľského vankúša s konkrétnou hustotou a obalom.",
        "V hustom jadre sa kvapalina pohybuje nielen cez stenu vlákna, ale aj medzivláknovými priestormi a švami. Tenzid mení zmáčanie a tlak pri používaní vytvára cesty do hĺbky. Pri sušení sa odparovanie na povrchu môže spomaliť, kým voda v strede ostáva. Preto je meranie času bez znalosti hrúbky, vlhkosti vzduchu a prúdenia nedostatočné.",
        "Porovnanie s páperím alebo polyesterom musí používať rovnaký výrobok a rovnakú skúšobnú metódu. Objem, návrat po stlačení, tepelný odpor, prenos vodnej pary a čas sušenia sú rozdielne veličiny. Jedna marketingová vlastnosť nemôže zastúpiť všetky. Pre domácnosť je najspoľahlivejší čitateľný návod, snímateľná bariéra a možnosť náplň skontrolovať alebo servisovať.",
    ],
    "source_intro": "Zdroje definujú kapok, opisujú jeho morfológiu a materiálové správanie a vysvetľujú symboly a štandardizované pranie. Z laboratórnej sorpcie nevytvárajú univerzálny návod na vankúš.",
    "sources": [
        ("Nariadenie EÚ 1007/2011: definícia kapoku", EU_FIBRE_LABEL),
        ("Peer-reviewed review: štruktúra a využitie kapokového vlákna", KAPOK_REVIEW),
        ("Peer-reviewed review: Ceiba pentandra a kapok", CEIBA_REVIEW),
        ("Recenzovaný prehľad prírodných vlákien", NATURAL_FIBRE_REVIEW),
        ("GINETEX: systém symbolov ošetrovania", GINETEX),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
    ],
    "product_heading": "Prací gél používajte na prateľný poťah, nie automaticky na kapokové jadro",
    "product_intro": "Najčastejším bezpečným krokom je vyprať samostatný snímateľný poťah podľa jeho etikety a výplň udržať suchú a vetranú.",
    "product_name": LAUNDRY_PRODUCT_NAME,
    "product_url": LAUNDRY_PRODUCT_URL,
    "product_text": "Gél možno presne dávkovať pri praní kompatibilného poťahu. Pred použitím skontrolujte farbu, zloženie, tvrdosť vody a pokyny produktu aj textílie.",
    "product_limit": "Prateľnosť poťahu neznamená prateľnosť kapokovej výplne. Na jadro produkt nepoužívajte bez výslovného návodu výrobcu a zaisteného úplného sušenia.",
    "category_heading": "Vyberte prostriedok pre oddeliteľnú prateľnú časť",
    "category_intro": "Kategória je relevantná pre poťahy a ďalšie textílie, ktoré majú povolené pranie. Kapokové jadro zostáva samostatným materiálovým systémom.",
    "category_name": LAUNDRY_CATEGORY_NAME,
    "category_url": LAUNDRY_CATEGORY_URL,
    "category_text": "Porovnajte pracie gély podľa materiálu poťahu, farby a dávkovania. Nepoužívajte vôňu ani väčšiu dávku na prekrytie vlhkého pachu z výplne.",
    "related": [
        ("Ako správne prať obliečky", ARTICLE_BEDDING),
        ("Ako vyprať páperový vankúš", ARTICLE_FEATHER_PILLOW),
        ("Ako prať cestovný vankúš a poťahy", ARTICLE_TRAVEL_PILLOW),
        ("Prečo textil po praní zapácha", ARTICLE_ODOR),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
        ("Ako čítať materiálový a ošetrovací štítok", ARTICLE_LABEL),
    ],
    "faq_title": "kapok, kapokové vankúše a výplne",
    "faq": [
        ("Čo je kapok?", "Kapok je ľahké plodové vlákno z vnútra toboliek stromu Ceiba pentandra. Používa sa najmä ako výplň a v zmesiach."),
        ("Je kapok to isté ako bavlna?", "Nie. Obe vlákna sú rastlinné, ale pochádzajú z odlišných častí rastlín a majú inú morfológiu aj spracovanie."),
        ("Dá sa kapokový vankúš prať v práčke?", "Iba ak to výslovne povoľuje výrobca celého jadra. Symbol na snímateľnom poťahu sa na výplň automaticky nevzťahuje."),
        ("Prečo sa kapok po namočení zhlukol?", "Voda a prostriedok sa mohli rozložiť nerovnomerne a tlak výplň stlačil. Najprv overte úplné vysušenie a až potom jemne upravujte objem podľa návodu."),
        ("Ako dlho schne kapokový vankúš?", "Neexistuje spoločný čas. Závisí od hrúbky, množstva vody, komôr, obalu, vlhkosti vzduchu a prúdenia. Kontrolujte stred, nie iba povrch."),
        ("Môžem použiť aviváž?", "Bez výslovného povolenia nie. Môže obaliť výplň, zostať v hĺbke a zhoršiť oplach aj omak."),
        ("Ako odstránim pach z kapoku?", "Najprv zistite, či je príčinou vlhkosť, znečistený poťah, biologická kontaminácia alebo degradácia. Pach neprekrývajte parfumom."),
        ("Čo robiť pri plesni na kapokovej výplni?", "Výrobok nepoužívajte a neriešte iba povrch. Pri viditeľnom raste, zdravotne citlivom používateľovi alebo zasiahnutom jadre zvoľte odborné posúdenie či výmenu."),
        ("Ako zabrániť presunu výplne?", "Vankúš pravidelne otáčajte a suchú výplň jemne preusporiadajte podľa návodu. Pri servisnom zipse možno upraviť množstvo na čistej ploche."),
    ],
}


SISAL: dict[str, object] = {
    "title": "Čo je sisal: pevné listové vlákno, škvrny od vody a čistenie kobercov",
    "link": "co-je-sisal-pevne-listove-vlakno-skvrny-od-vody-a-cistenie-kobercov",
    "meta": "Čo je sisal, ako ho odlíšiť od juty a syntetickej napodobeniny a ako čistiť sisalový koberec od prachu a škvŕn bez zbytočného premočenia.",
    "short": "Sisal je pevné rastlinné vlákno z listov agávy Agave sisalana. V koberci môže byť citlivé na veľké množstvo vody, nerovnomerné schnutie, migráciu nečistôt z podkladu a vznik máp. Základom je pravidelné suché vysávanie a rýchle kontrolované odsatie rozliatej tekutiny.",
    "name": "sisal",
    "locative": "sisale",
    "identity_heading": "Sisal je listové vlákno z agávy, nie juta ani morská tráva",
    "identity_detail": "Nariadenie EÚ definuje sisal ako vlákno získané z listov Agave sisalana. FAO ho radí medzi tvrdé prírodné vlákna a uvádza využitie v povrazoch, rohožiach a kobercoch.",
    "identity_boundary": "Koberec predávaný v sisalovom vzhľade však môže byť z polypropylénu alebo inej syntetiky. Pravý sisal môže mať latexový, textilný alebo penový podklad, farbenú bordúru a lepené spoje s odlišnou citlivosťou.",
    "label_focus": "potvrdenie pravého sisalu alebo syntetickej napodobeniny, materiál podkladu a bordúry, povolené lokálne či profesionálne čistenie, zákaz premočenia a odporúčaný smer vysávania",
    "missing_label": "Pri neznámom koberci najprv identifikujte výrobcu alebo predajcu. Celoplošné mokré čistenie bez údajov je rizikové, pretože reakciu viditeľného vlákna nemožno oddeliť od podkladu a lepidla.",
    "dry_check": "uvoľnené slučky alebo vlákna, zvlnené hrany, staré vodné kruhy, hnednutie, prasknutú bordúru, mäkký podklad, piesok pri väzbe a známky vlhkosti na podlahe",
    "damage_boundary": "Suchý piesok sa dá odstrániť vysávaním, ale zvlnený podklad, zrazená bordúra alebo trvalo stmavnutá vodná mapa nie sú voľná nečistota, ktorú treba viac drhnúť.",
    "test_focus": "Pri sisale sledujte po vyschnutí aj hranicu mokrej zóny, zmenu tuhosti, zvlnenie a presun farby z bordúry alebo podkladu, nie iba zmiznutie samotnej škvrny.",
    "combined_risk": "napučania celulózového vlákna, kapilárneho presunu rozpustných látok, nerovnomerného vysušenia a zmeny rozmeru podkladu alebo okrajovej väzby",
    "chemistry_boundary": "Roztok aplikujte iba v množstve a spôsobom povoleným výrobcom koberca. Čistič určený všeobecne na koberce nie je automaticky schválený pre prírodný sisal.",
    "drying_detail": "Po lokálnom zásahu musí vzduch cirkulovať nad kobercom aj pri podlahe, ak konštrukcia umožňuje bezpečné nadvihnutie. Mokrá zóna nemá zostať prikrytá nábytkom ani nepriedušnou podložkou.",
    "heat_boundary": "Horúci prúd môže vysušiť okraj rýchlejšie než stred, zafixovať mapu, zmeniť bordúru a poškodiť latex, lepidlo alebo povrch podlahy.",
    "stop_signs": "rozširujúca sa hnedá mapa, prenos farby, zvlnenie, tvrdnutie vlákna, lepkavý podklad, oddeľovanie bordúry alebo vlhkosť preniknutá na citlivú podlahu",
    "professional_boundary": "Malé čerstvé rozliatie možno často okamžite odsať, ale staré mapy, zápach moču, veľká plocha, zatečený podklad a pevne položený sisal si vyžadujú firmu, ktorá výslovne pozná prírodné rastlinné koberce.",
    "answer": "Sisal je pevné rastlinné vlákno získané z listov agávy Agave sisalana. Na koberci najprv pravidelne odstraňujte suchý prach a piesok vysávačom nastaveným podľa odporúčania výrobcu. Rozliatu tekutinu okamžite odsávajte bielou savou handričkou bez drhnutia a bez zväčšovania mokrej plochy. Pravý sisal nečistite veľkým množstvom vody, parou ani univerzálnym kobercovým postupom, kým výrobca nepotvrdí kompatibilitu viditeľného vlákna, farbiva, bordúry, lepidla a podkladu. Každý prípravok vyskúšajte na skrytom mieste a výsledok posúďte až po úplnom vyschnutí. Pri hnednutí, zvlnení, zápachu z podkladu alebo veľkom premočení zavolajte odborníka na prírodné koberce.",
    "intro": "Pri otázke ako vyčistiť sisalový koberec je najväčšou chybou predpoklad, že koberec je jedna homogénna vrstva. Viditeľná väzba môže byť z pravého sisalu, no pod ňou je podklad, lepidlo a podlaha; okraj môže byť z bavlny alebo syntetiky. Voda napučí rastlinné vlákno a rozpustené nečistoty sa pri schnutí môžu presunúť k okraju, kde zostane kruh. Preto sa bežná rutina opiera hlavne o suché odstránenie častíc a pri nehode o rýchle odsatie. Mokré čistenie musí byť kontrolované, materiálovo potvrdené a spojené s okamžitým rovnomerným sušením.",
    "quick": [
        "<strong>Sisal pochádza z listov agávy:</strong> nie je to juta, kokosové vlákno ani morská tráva.",
        "<strong>Sisalový vzhľad môže byť syntetický:</strong> pred čistením overte štítok a podklad.",
        "<strong>Suchá údržba je základ:</strong> piesok a prach odstraňujte skôr, než sa vtlačia do väzby.",
        "<strong>Tekutinu odsávajte, nedrhnite:</strong> veľká mokrá zóna zvyšuje riziko hnednutia a máp.",
        "<strong>Čistič kobercov nie je automatické povolenie:</strong> musí byť vhodný pre konkrétny sisal a jeho podklad.",
        "<strong>Schnutie musí byť rovnomerné:</strong> nábytok ani nepriedušná podložka nemajú zakryť vlhké miesto.",
    ],
    "overview_heading": "Čo je sisal a prečo sa používa na koberce a rohože",
    "overview": [
        "Sisalové vlákna sa získavajú z dlhých listov rastliny Agave sisalana. Po oddelení rastlinného tkaniva sa vlákna čistia, sušia a ďalej spracúvajú. FAO ich zaraďuje medzi tvrdé vlákna a opisuje ich technické využitie. Pevnosť a tuhší omak sú výhodou pri povrazoch, rohožiach a podlahových krytinách, no neznamenajú necitlivosť na vodu alebo chemické čistenie.",
        "Celulózové vlákno prijíma vlhkosť a môže meniť rozmer aj optický vzhľad. Voda navyše rozpúšťa prirodzené alebo uložené látky a pri odparovaní ich presúva. Keď okraj schne inou rýchlosťou než stred, vznikne viditeľná mapa. Opakované premáčanie môže meniť tuhosť a farbu, zatiaľ čo podklad sa môže zvlniť alebo oddeliť.",
        "Syntetický koberec s plochou väzbou môže kopírovať prírodný vzhľad, ale mať úplne inú odolnosť. Ani syntetika však nedáva automatické povolenie na každý čistič, pretože farbivo, podklad a lepidlo ostávajú rozhodujúce. Identifikácia nie je akademický detail: určuje, či možno použiť vodný roztok, akú plochu ošetriť a ako rýchlo treba odvádzať vlhkosť.",
    ],
    "table1_heading": "Sisal, juta, kokosové vlákno, morská tráva a syntetická napodobenina",
    "table1_intro": "Prírodné podlahové materiály sa často predávajú vedľa seba. Typický vzhľad pomôže s orientáciou, ale rozhodnutie o čistení musí potvrdiť výrobca.",
    "table1_headers": ["Materiál", "Pôvod", "Typický povrch", "Riziko pri mokrom čistení"],
    "table1_rows": [
        ("Sisal", "Listy Agave sisalana.", "Pevný, tuhší, zreteľne vláknitý a vhodný na husté väzby.", "Napučanie, hnednutie, vodné mapy a zmena podkladu."),
        ("Juta", "Lýko rastlín rodu Corchorus.", "Mäkší, často hrubší a rustikálny povrch.", "Citlivosť na premočenie, zmenu farby a dlhú vlhkosť."),
        ("Kokosové vlákno", "Vlákna z obalu kokosového plodu.", "Hrubé, pružné až kefovité vlákna typické pre rohožky.", "Zadržiavanie piesku, nerovnomerné schnutie a podklad."),
        ("Morská tráva", "Sušené rastlinné steblá alebo listové časti.", "Hladší, pevný a často prirodzene lesklý povrch.", "Mapa, zmena farby a reakcia spojov; presné pokyny sa líšia."),
        ("Syntetický sisalový vzhľad", "Najčastejšie polypropylén alebo iný polymér.", "Napodobňuje plochú väzbu a prírodný odtieň.", "Vlákno môže zniesť viac, no farbivo, lepidlo a podklad stále limitujú postup."),
    ],
    "sections": [
        {
            "heading": "Konštrukcia koberca: vlákno, väzba, podklad a bordúra",
            "paragraphs": [
                "Viditeľná sisalová priadza sa môže tkať do rybej kosti, slučiek alebo plochej väzby. Hustota určuje, koľko piesku prenikne medzi rebrá a ako ľahko sa dá odsávať. Rub stabilizuje textilný, latexový alebo iný podklad a okraj chráni bordúra. Každá časť reaguje na vodu, teplo a ťah odlišne.",
                "Voľne položený koberec možno pri nehode skontrolovať aj zospodu, pevne nalepenú krytinu nie. Bordúra môže pustiť farbu do svetlého sisalu a podklad môže preniesť rozpustné látky na povrch. Preto skúšku nerobte iba v strede vlákna; ak plánujete čistiť pri okraji, overte aj spoj a bordúru.",
            ],
        },
        {
            "heading": "Pravidelné vysávanie sisalu bez vyťahania vlákien",
            "paragraphs": [
                "Piesok pôsobí pri chôdzi ako brúsivo, preto je pravidelné vysávanie najdôležitejší preventívny krok. Použite sací výkon, nadstavec a prípadnú rotačnú kefu podľa pokynu výrobcu. Agresívna kefa môže rozstrapkať vystupujúce vlákna alebo zachytiť slučku. Vysávajte pomaly vo viacerých smeroch, no poškodené miesto najprv stabilizujte.",
                "Pred vysávačom odstráňte väčšie kúsky rukou alebo tupou lopatkou, aby nepoškodili väzbu ani zariadenie. Pri jemnom prášku neudrite do koberca, lebo ho zatlačíte hlbšie. Okraje a priestor pod nábytkom čistite pravidelne; rozdielna záťaž a prach môžu vytvoriť farebný kontrast, ktorý sa neskôr mylne považuje za škvrnu.",
            ],
        },
        {
            "heading": "Čo urobiť hneď po rozliatí vody alebo nápoja",
            "paragraphs": [
                "Odstráňte pevný zvyšok bez zatláčania a tekutinu odsávajte čistou bielou handričkou. Prikladajte ju zhora a podľa možnosti aj z rubu. Netrieť do strán znamená obmedziť mokrú plochu a mechanické poškodenie. Farebný nápoj môže mať viac zložiek; najprv odstráňte objem tekutiny a až potom voľte kompatibilné lokálne čistenie.",
                "Pod koberec vložte vhodnú ochrannú savú vrstvu iba vtedy, ak ho možno bezpečne nadvihnúť bez zlomenia alebo odtrhnutia. Chráňte drevenú či laminátovú podlahu pred vodou. Použitú handričku často meňte, aby ste tekutinu nevracali. Miesto nezakrývajte a zabezpečte vzduch bez prudkého lokálneho tepla.",
            ],
            "callout": {
                "title": "Prvých päť minút po rozliatí",
                "items": [
                    "Odstráňte pevné zvyšky bez škrabania vlákien.",
                    "Tekutinu odsávajte bielou savou handričkou, nie farebným uterákom.",
                    "Nerozširujte mokrú zónu drhnutím ani veľkým množstvom vody.",
                    "Skontrolujte rub, podklad a podlahu, ak je to bezpečne dostupné.",
                    "Začnite rovnomerné sušenie a sledujte hranicu mapy.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Prečo vzniká vodný kruh a hnednutie sisalu",
            "paragraphs": [
                "Počas navlhčenia sa rozpustné látky môžu presúvať vodou. Keď odparovanie prebieha hlavne na hranici, materiál sa tam koncentruje a vytvorí kruh. Hnednutie môže pochádzať z rastlinného vlákna, starých nečistôt, podkladu alebo reakcie produktu. Ďalšia malá mokrá bodka uprostred často iba vytvorí nový okraj.",
                "Oprava mapy niekedy vyžaduje rovnomernejšie ošetrenie väčšej prirodzene ohraničenej zóny a kontrolovanú extrakciu, čo už patrí odborníkovi. Domáce rozširovanie vody bez zariadenia zvyšuje riziko. Najprv zdokumentujte tvar, použitý produkt a čas schnutia. Tieto údaje pomôžu rozlíšiť prenesenú nečistotu od zmeny farby alebo podkladu.",
            ],
        },
        {
            "heading": "Mastná škvrna, jedlo a blato na sisalovom koberci",
            "paragraphs": [
                "Blato nechajte zaschnúť, ak tým nehrozí ďalšie zašliapanie, potom ho rozrušte a povysávajte podľa odporúčania výrobcu. Mokré roztieranie prenesie jemné častice hlbšie. Mastnotu najprv zachyťte savým materiálom bez silného tlaku. Kuchynský odmasťovač nemusí byť vhodný pre farbivo, prírodné vlákno ani podklad.",
                "Pri potravine oddeľte pevný zvyšok, vodnú časť, tuk a pigment. Jediný zásah nemusí riešiť všetko. Lokálny kobercový prípravok použite len po potvrdení kompatibility so sisalom a po skrytej skúške. Dodržte množstvo a spôsob odstránenia zvyšku, pretože lepkavý tenzid bude po vyschnutí viazať nový prach.",
            ],
        },
        {
            "heading": "Moč domácich zvierat a opakovaný zápach",
            "paragraphs": [
                "Moč môže preniknúť cez väzbu do podkladu a podlahy. Povrchové prevoňanie preto nevyrieši zdroj a pri opakovanej vlhkosti sa pach vracia. Čerstvú tekutinu odsajte, zabráňte zvieraťu v ďalšom kontakte a zistite rozsah z rubu. Enzymatický produkt nie je automaticky bezpečný pre každý prírodný koberec; kompatibilitu musí potvrdiť výrobca.",
                "Staré alebo opakované zasiahnutie často presahuje možnosti bodového čistenia. Odborná firma musí vedieť pracovať s prírodným sisalom, nie iba ponúkať všeobecné tepovanie. Pri pevnej krytine môže byť potrebné posúdiť aj podklad a podlahu. Ak sa zápach vráti po zvýšení vlhkosti vzduchu, je to znak, že zdroj pravdepodobne zostal v hĺbke.",
            ],
        },
        {
            "heading": "Môže sa sisalový koberec tepovať alebo čistiť parou",
            "paragraphs": [
                "Nie ako všeobecné pravidlo. Tepovanie pridáva vodu a mechaniku, para teplo a kondenzáciu. Pravý sisal, farbivo, bordúra, latexový podklad aj lepidlo môžu reagovať odlišne. Ak výrobca uvádza konkrétny profesionálny nízkovlhkostný postup, dodržte ho; bežný program stroja pre syntetický koberec nemožno preniesť podľa vzhľadu.",
                f"Návod <a href=\"{ARTICLE_RUG}\">ako vyčistiť koberec</a> vysvetľuje všeobecné kroky, no pri sisale má materiálová hranica prednosť. Malý skrytý test musí úplne vyschnúť a neukazovať mapu, zmenu tuhosti ani prenos farby. Pri veľkej ploche najprv získajte písomné odporúčanie výrobcu alebo odborníka na prírodné rastlinné krytiny.",
            ],
        },
        {
            "heading": "Sisalový koberec nepatrí automaticky do práčky",
            "paragraphs": [
                f"Pružný malý rozmer nie je dôkaz prateľnosti. Článok <a href=\"{ARTICLE_WASHABLE_RUG}\">ako prať koberce v práčke</a> zdôrazňuje kapacitu, podklad a etiketu. Sisalové vlákno môže napučať, okraj sa zraziť a podklad prasknúť alebo sa oddeliť. Mokrá rohož je ťažká a odstreďovanie ju zaťaží nerovnomerne.",
                "Ak štítok výslovne povoľuje strojové pranie, overte, či ide o pravý sisal alebo syntetickú napodobeninu a dodržte kapacitu zariadenia. Neimprovizujte pri ručne lemovanom, pevne tvarovanom alebo lepenom kuse. Absencia zákazu nie je povolenie a nízka teplota neodstraňuje mechanické riziko.",
            ],
        },
        {
            "heading": "Sušenie koberca a ochrana podlahy",
            "paragraphs": [
                "Po lokálnom zásahu odsajte čo najviac povolenej vlhkosti a nechajte miesto otvorené. Ventilátor môže podporiť výmenu vzduchu v miestnosti, no nemá fúkať horúci prúd na jeden okraj. Ak koberec bezpečne nadvihnete, kontrolujte podklad a chráňte podlahu čistou suchou savou vrstvou, ktorú pravidelne meníte.",
                "Koberec nevracajte pod nábytok, kým nie je suchý. Nohy nábytku môžu preniesť farbu alebo vytvoriť tlakový okraj. Pri pevne položenej krytine sledujte zápach a vlhkosť aj pri sokli. Ak podklad mäkne, lepí sa alebo zostáva chladný, zastavte domáci zásah a zabezpečte odborné vysušenie.",
            ],
        },
        {
            "heading": "Vystupujúce vlákno, slučka a rozstrapkaná bordúra",
            "paragraphs": [
                "Vystupujúcu nosnú niť nevyťahujte a neodstrihujte bez posúdenia väzby. Môže držať viac radov a jej skrátenie otvorí ďalšiu plochu. Voľný koniec pri bordúre stabilizujte tak, aby sa nezachytil do vysávača, a zverte opravu kobercovému servisu. Domáce lepidlo môže vytvoriť tvrdú škvrnu a poškodiť podlahu.",
                "Rozstrapkaný povrch po agresívnej kefe už nie je prach. Znížte mechaniku a porovnajte nepoškodenú zónu. Pri novom koberci zdokumentujte stav, nadstavec a odporúčanie výrobcu. Prirodzená nepravidelnosť rastlinného vlákna je odlišná od postupujúceho rozpletenia alebo diery v nosnej väzbe.",
            ],
        },
        {
            "heading": "Umiestnenie sisalu, vlhkosť a prevencia škvŕn",
            "paragraphs": [
                "Sisal nie je ideálny do zóny s pravidelným premočením, ak výrobca výslovne neuvádza vhodnú konštrukciu. Pri vstupe zachyťte mokré topánky samostatnou rohožkou a pri jedálenskom stole reagujte na rozliatie okamžite. Kvetináč postavte na stabilnú nepriepustnú podložku s kontrolou kondenzácie, nie priamo na koberec.",
                "Relatívna vlhkosť a studená podlaha ovplyvňujú čas schnutia. Koberec pravidelne skontrolujte pod nábytkom a pri vonkajšej stene. Ochranný sprej používajte len po potvrdení výrobcu; môže zmeniť farbu, priľnavosť nečistôt aj budúce čistenie. Prevencia má byť kompatibilná s celým systémom, nie iba s názvom vlákna.",
            ],
        },
        {
            "heading": "Ako vybrať sisalový koberec do domácnosti",
            "paragraphs": [
                "Pýtajte sa na presný materiál, pôvod vlákna, typ väzby, bordúru, podklad, protišmykové riešenie a písomný návod na škvrny. Vzorku si pozrite v dennom aj umelom svetle a jemne po nej prejdite rukou, aby ste posúdili tuhosť. Na schody, detskú izbu a domácnosť so zvieraťom treba hodnotiť aj opraviteľnosť a reálnu údržbu.",
                "Kvalitná ponuka má vysvetliť rozdiel medzi pravým sisalom a syntetickou alternatívou. Syntetický vzhľad môže byť praktickejší v rizikovej zóne, no nie je to menejcenná voľba, ak lepšie sedí použitiu. Rozhodujte podľa kontaktu s vodou, intenzity chôdze, slnka, podlahového kúrenia a dostupnosti špecializovaného čistenia.",
            ],
        },
    ],
    "table2_heading": "Sisalový koberec po nehode: príčina a ďalší krok",
    "table2_intro": "Zmenu hodnotíte až po rovnomernom vyschnutí. Pri zväčšovaní mapy alebo poškodení podkladu nepokračujte ďalším domácim roztokom.",
    "table2_headers": ["Prejav", "Pravdepodobná skupina príčin", "Čo skontrolovať", "Ďalší krok"],
    "table2_rows": [
        ("Hnedý kruh okolo čistenej plochy", "Migrácia rozpustných látok k okraju pri schnutí.", "Použitý objem, podklad, rýchlosť a smer schnutia.", "Nezväčšovať bodové zmáčanie; konzultovať rovnomerné odborné ošetrenie."),
        ("Vlákno je tvrdšie alebo drsnejšie", "Zvyšok produktu, mechanické poškodenie alebo zmena po vode.", "Skrytý test, lepkavosť a prenos na bielu handričku.", "Nedrhnúť; pri povolení kontrolovane odstrániť zvyšok."),
        ("Koberec sa vlní", "Nerovnomerné napučanie, zrazenie bordúry alebo poškodenie podkladu.", "Rub, okraje a rovinu podlahy.", "Sušiť bez zaťaženia a vyžiadať odborné posúdenie."),
        ("Pach sa vracia", "Vlhkosť alebo kontaminácia v podklade a podlahe.", "Spodnú stranu, sokel a históriu nehody.", "Neprevoňať; lokalizovať a odborne odstrániť zdroj."),
        ("Bordúra pustila farbu", "Nestálofarebný materiál a presun vlhkosti.", "Rozsah, skrytú skúšku a spoj bordúry.", "Zastaviť vodu, odsávať bez trenia a kontaktovať odborníka."),
    ],
    "steps_heading": "Ako postupovať pri čerstvej škvrne na sisale",
    "steps": [
        "Overte, či ide o pravý sisal, a nájdite pokyny výrobcu pre vlákno, bordúru a podklad.",
        "Odstráňte pevný zvyšok tupou hranou bez škrabania a bez zatláčania do väzby.",
        "Tekutinu opakovane odsávajte čistou bielou handričkou bez pohybu do strán.",
        "Skontrolujte rub a podlahu, ak možno koberec bezpečne nadvihnúť bez poškodenia.",
        "Kompatibilný produkt použite až po skrytej skúške a iba v množstve povolenom výrobcom.",
        "Zvyšok roztoku odstráňte určeným spôsobom a nevytvárajte ďalšiu veľkú mokrú zónu.",
        "Zabezpečte rovnomerné prúdenie vzduchu bez horúceho fénu, radiátora a prikrytia nábytkom.",
        "Po vyschnutí porovnajte farbu, tuhosť, rovinu, bordúru a pach; pri zmene zásah neopakujte.",
    ],
    "remember": [
        "Je koberec z pravého sisalu alebo zo syntetiky v sisalovom vzhľade?",
        "Aký má podklad, bordúru, lepidlo a spôsob položenia?",
        "Povoľuje výrobca vodný čistič, nízkovlhkostný postup alebo iba odborné čistenie?",
        "Prenáša sa farba alebo mení tuhosť po úplnom vyschnutí skrytej skúšky?",
        "Dostala sa tekutina na podlahu alebo do podkladu?",
        "Dokáže miesto schnúť odkryté a rovnomerne zhora aj odspodu?",
    ],
    "mistakes": [
        "Zameniť pravý sisal za syntetický koberec podľa podobného vzhľadu.",
        "Drhnúť rozliatu tekutinu a zväčšiť mokrú aj mechanicky poškodenú plochu.",
        "Použiť parný čistič alebo tepovač bez výslovného materiálového povolenia.",
        "Aplikovať univerzálny kobercový produkt bez skúšky vlákna, bordúry a podkladu.",
        "Vysušiť iba povrch a prikryť vlhký podklad nábytkom.",
        "Prekryť moč alebo zatuchnutie vôňou bez odstránenia zdroja z hĺbky.",
    ],
    "expert_heading": "Odbornejší pohľad: listové vlákno, kapilárny presun a rozhranie vrstiev",
    "expert": [
        "Sisal je viacbunkové listové vlákno, ktorého pevnosť, jemnosť a povrch závisia od pestovania a spracovania. FAO materiály opisujú technické vlastnosti a možnosti využitia; nie sú spotrebiteľským protokolom čistenia koberca. Pri podlahovej krytine treba k vláknu pripočítať priadzu, väzbu, farbivo, podklad a podmienky používania.",
        "Vodná mapa súvisí s transportom kvapaliny a rozpustených látok. Kapilárny tok, gravitačné prenikanie a odparovanie sa menia podľa hustoty, povrchovej úpravy a kontaktu s podkladom. Ak sa jedna zóna suší rýchlejšie, okraj môže koncentrovať látky. Preto agresívne lokálne premočenie bez kontrolovanej extrakcie vytvára nový problém aj pri chemicky vhodnom produkte.",
        "AATCC TM215 hodnotí uvoľnenie pôdy pri domácom praní textílií v definovanom postupe, no pevne položený sisalový koberec neprechádza tým istým systémom. Laboratórne a produktové skúšky treba čítať podľa rozsahu. Pre domácnosť je relevantné výslovné povolenie výrobcu koberca, úspešná skrytá skúška po vyschnutí a schopnosť odstrániť vodu zo všetkých vrstiev.",
    ],
    "source_intro": "Zdroje definujú sisal, opisujú tvrdé rastlinné vlákna a štandardizované hodnotenie textilu. Konkrétny čistiaci postup musí potvrdiť výrobca koberca a jeho vrstiev.",
    "sources": [
        ("Nariadenie EÚ 1007/2011: definícia sisalu", EU_FIBRE_LABEL),
        ("FAO: jute and hard fibres", FAO_HARD_FIBRES),
        ("FAO: vlastnosti a spracovanie sisalu", FAO_SISAL),
        ("FAO: použitie sisalu v kobercoch a rohožiach", FAO_SISAL_USES),
        ("Recenzovaný prehľad prírodných vlákien", NATURAL_FIBRE_REVIEW),
        ("AATCC TM215: odstránenie pôdy pri praní", AATCC_SOIL),
    ],
    "product_heading": "Lokálny čistič použite iba na výrobcom potvrdený koberec",
    "product_intro": "Pri kompatibilnom a farebne stálom koberci môže lokálny prípravok pomôcť so škvrnou, no pravý prírodný sisal potrebuje výslovné potvrdenie a malú úplne vysušenú skúšku.",
    "product_name": RUG_PRODUCT_NAME,
    "product_url": RUG_PRODUCT_URL,
    "product_text": "Prípravok je určený na škvrny na kobercoch. Pred použitím prečítajte jeho návod aj ošetrovacie pokyny koberca a overte reakciu na skrytom mieste vrátane bordúry a podkladu.",
    "product_limit": "Nepovažujte tento produkt za automaticky vhodný pre pravý sisal. Použite ho iba vtedy, keď výrobca konkrétneho koberca potvrdzuje kompatibilný postup; pri nejasnom prírodnom vlákne zvoľte odborníka.",
    "category_heading": "Čistiace prostriedky vyberajte podľa povrchu, nie iba typu škvrny",
    "category_intro": "Rôzne povrchy potrebujú odlišné pH, množstvo vody, kontaktný čas a odstránenie zvyšku. Pri sisale je materiálová vhodnosť dôležitejšia než univerzálne označenie na prednej strane.",
    "category_name": CLEANING_CATEGORY_NAME,
    "category_url": CLEANING_CATEGORY_URL,
    "category_text": "V kategórii nájdete riešenia pre rozličné úlohy v domácnosti. Pred aplikáciou na koberec vždy porovnajte určenie produktu s vláknom, farbou, bordúrou a podkladom.",
    "related": [
        ("Ako vyčistiť koberec", ARTICLE_RUG),
        ("Kedy možno prať koberec v práčke", ARTICLE_WASHABLE_RUG),
        ("Ako vyčistiť rohožku a textílie v predsieni", ARTICLE_DOORMAT),
        ("Ako odstrániť rôzne škvrny z textilu", ARTICLE_STAIN),
        ("Prečo farby blednú pri praní a trení", ARTICLE_COLOR),
        ("Prečo sa po čistení vracia zápach", ARTICLE_ODOR),
    ],
    "faq_title": "sisal a čistenie sisalových kobercov",
    "faq": [
        ("Čo je sisal?", "Sisal je pevné rastlinné vlákno získané z listov agávy Agave sisalana. Používa sa na povrazy, rohože a koberce."),
        ("Ako zistím, či je koberec z pravého sisalu?", "Najspoľahlivejší je štítok alebo potvrdenie výrobcu. Vzhľad nestačí, pretože syntetické vlákna môžu sisal presvedčivo napodobniť."),
        ("Môže sa sisalový koberec tepovať?", "Nie bez výslovného povolenia. Veľké množstvo vody môže spôsobiť mapy, hnednutie, zvlnenie a poškodenie podkladu."),
        ("Ako odstránim vodnú škvrnu zo sisalu?", "Ďalším bodovým namáčaním môžete vytvoriť nový kruh. Zdokumentujte mapu a obráťte sa na odborníka, ktorý pozná prírodné koberce a riadené nízkovlhkostné čistenie."),
        ("Môžem použiť parný čistič?", "Iba ak ho povoľuje výrobca konkrétneho koberca. Para pridáva teplo a kondenzáciu, ktoré môžu zmeniť vlákno, bordúru aj podklad."),
        ("Čo robiť po rozliatí vína alebo vody?", "Tekutinu okamžite odsávajte bielou handričkou bez drhnutia, skontrolujte rub a podlahu a zabezpečte rovnomerné sušenie."),
        ("Ako často sisal vysávať?", "Podľa intenzity chôdze a množstva prachu. V exponovaných zónach pravidelne, aby piesok nepôsobil ako brúsivo medzi vláknami."),
        ("Prečo sisal po čistení zapácha?", "Vlhkosť alebo nečistota mohla zostať v podklade či podlahe. Pach neprekrývajte; lokalizujte zdroj a zaistite úplné vysušenie."),
        ("Je syntetický sisalový vzhľad jednoduchší na údržbu?", "Často môže lepšie znášať vodu, ale aj pri ňom rozhoduje farbivo, podklad, lepidlo a pokyn výrobcu."),
    ],
}


INTERLINING: dict[str, object] = {
    "title": "Čo je vlizelín: výstuž odevu, lepidlo a bezpečné čistenie",
    "link": "co-je-vlizelin-vystuz-odevu-lepidlo-a-bezpecne-cistenie",
    "meta": "Čo je vlizelín, ako sa líši nažehľovací a všívací variant a prečo sa na golieri, manžete či saku po praní tvoria bubliny a odlepené miesta.",
    "short": "Vlizelín je bežné pomenovanie výstužného materiálu medzi vrstvami odevu. Môže byť nažehľovací alebo všívací a jeho nosič môže byť netkaný, tkaný či pletený. Pri starostlivosti rozhoduje celý spoj: vrchná látka, lepidlo, teplota fixácie, výstuž, šev aj podšívka.",
    "name": "vlizelín",
    "locative": "vlizelíne",
    "identity_heading": "Vlizelín opisuje funkciu výstuže, nie jednu surovinu",
    "identity_detail": "V bežnej reči sa tak označuje medzivrstva, ktorá spevňuje golier, manžetu, pás, légu alebo celú prednú časť odevu. Nosič môže byť netkaný, tkaný alebo pletený a môže sa prišiť alebo teplom nalepiť.",
    "identity_boundary": "Výraz nažehľovací hovorí o spôsobe spojenia, nie o tom, že hotový odev možno ľubovoľne prežehľovať. Lepidlo má definovanú teplotu, tlak a čas aktivácie a po spojení musí zostať kompatibilné s vrchnou látkou aj budúcim čistením.",
    "label_focus": "vrchnú látku, podšívku, výstuž v rôznych zónach, lepené a všívané časti, tvar ramien, chlopne, golier, výšivku a symbol profesionálneho čistenia",
    "missing_label": "Pri hotovom saku, kabáte alebo spoločenskom odeve bez návodu nepredpokladajte, že nízka teplota ochráni vnútorné lepenie. Konštrukciu nemožno spoľahlivo určiť bez otvorenia odevu.",
    "dry_check": "bubliny, vlny, odlepené rohy, presvitajúce bodky lepidla, stuhnutý lem, rozdielne zrazenie vrstiev, zvlnenú légu a deformovaný golier alebo chlopňu",
    "damage_boundary": "Bublina po praní môže znamenať delamináciu alebo rozdielne zrazenie, nie povrchovú škvrnu. Ďalší horúci cyklus bez znalosti lepidla môže poškodenie zväčšiť.",
    "test_focus": "Na hotovom odeve skúška musí sledovať nielen farbu líca, ale aj zmenu tuhosti, vznik vlny a obrys lepiacich bodov po úplnom ochladení a ustálení.",
    "combined_risk": "rozdielneho napučania a zrážania vrchnej látky a výstuže, mäknutia lepidla, tlaku pri žehlení a ťahu v šve",
    "chemistry_boundary": "Prací alebo odškvrňovací produkt vyberajte pre celý odev. Rozpúšťadlo bezpečné pre vlákno môže ovplyvniť lepidlo a vodný roztok môže deformovať štruktúrovanú prednú časť.",
    "drying_detail": "Sako, kabát alebo vystuženú košeľu vytvarujte podľa švov a podoprite tak, aby mokrá hmotnosť neťahala golier, chlopňu, pás ani okraj lepeného dielu.",
    "heat_boundary": "Teplo môže zraziť vrchnú látku, zmäkčiť alebo znovu aktivovať lepidlo, vytlačiť ho na líc a zafixovať deformáciu pod tlakom.",
    "stop_signs": "rastúce bubliny, oddeľujúca sa vrstva, lepkavý povrch, presiaknuté lepidlo, zvrásnený golier, krútiaca sa léga alebo prenos farby z podšívky",
    "professional_boundary": "Malá všívaná výstuž v prateľnej košeli môže zniesť domáci cyklus podľa etikety, no lepená predná časť saka, kabáta alebo hodnotného odevu patrí pri poruche krajčírovi či čistiarni so skúsenosťou s interliningom.",
    "answer": "Vlizelín je bežné pomenovanie výstužnej medzivrstvy, ktorá pomáha držať tvar goliera, manžety, pásu, légy, chlopne alebo výšivky. Nie je to jedno vlákno ani vždy netkaná textília. Môže byť nažehľovací s lepiacimi bodmi alebo všívací bez lepidla a jeho nosič môže byť tkaný, pletený či netkaný. Hotový odev perte iba podľa vlastného štítku, pretože vrchná látka, výstuž, lepidlo, podšívka a švy musia zniesť rovnaký proces. Pri bublinách, odlepovaní alebo zvlnení nepridávajte automaticky teplo. Odev nechajte úplne vyschnúť, zdokumentujte zmenu a pri saku, kabáte alebo hodnotnom kuse vyhľadajte krajčíra či odbornú čistiareň.",
    "intro": "Otázka čo je vlizelín často vznikne pri šití, no spotrebiteľ ho objaví až vtedy, keď sa po praní zvlni golier alebo na prednej časti saka vzniknú bubliny. Viditeľná látka môže byť bez chyby; problém je na rozhraní vrstiev. Lepiace body potrebujú pri výrobe správnu teplotu, tlak a čas a spojené materiály musia mať podobnú rozmerovú stabilitu. Pri údržbe sa preto nedá vychádzať iba zo zloženia líca. Bezpečný postup rešpektuje celý odev, symboly výrobcu a hranicu medzi bežným pokrčením a skutočným oddelením výstuže.",
    "quick": [
        "<strong>Vlizelín je výstužná vrstva:</strong> pomáha držať tvar, spevňuje okraj alebo stabilizuje plochu.",
        "<strong>Nie je vždy netkaný:</strong> nosič môže byť tkaný, pletený alebo netkaný.",
        "<strong>Nažehľovací a všívací variant sú odlišné:</strong> prvý pridáva lepidlo, druhý držia stehy a konštrukcia.",
        "<strong>Etiketa patrí celému odevu:</strong> symbol vrchnej látky nemožno čítať oddelene od výstuže a podšívky.",
        "<strong>Bublina nie je obyčajný záhyb:</strong> môže ísť o delamináciu alebo rozdielne zrazenie vrstiev.",
        "<strong>Viac tepla nie je automatická oprava:</strong> bez parametrov lepidla môže vzniknúť presiaknutie alebo trvalá deformácia.",
    ],
    "overview_heading": "Na čo slúži výstuž a čo sa skrýva medzi vrstvami odevu",
    "overview": [
        "Medzivrstva upravuje tuhosť, stabilitu, návrat tvaru a správanie pri šití. V golieri bráni mäkkému prepadnutiu, pri gombíkovej lége obmedzuje vyťahanie, v páse rozkladá tlak a v chlopni pomáha držať líniu. Rovnaká úloha sa dá dosiahnuť ľahkým netkaným materiálom, pružnou pletenou výstužou, tkaným plátnom alebo viacerými vrstvami všívaného krajčírskeho systému.",
        "Nažehľovací variant má na jednej strane termoplastické lepidlo nanesené v bodoch, mriežke alebo inom vzore. Pri fixácii sa aktivuje definovanou kombináciou teploty, tlaku a času a po ochladení spojí vrstvy. Ak je teplota nízka, čas krátky alebo tlak nerovnomerný, spoj môže byť slabý. Ak je teplo alebo tlak priveľký, lepidlo môže presiaknuť alebo zmeniť omak.",
        "ISO 9092:2026 poskytuje aktuálnu slovnú zásobu pre netkané textílie, no vlizelín v bežnom zmysle nemožno zúžiť iba na túto kategóriu. Technické materiály Vlieseline a Freudenberg rozlišujú výrobky podľa nosiča, hmotnosti, pružnosti, účelu a spôsobu aplikácie. Pre spotrebiteľa je kľúčové vedieť, že vnútorná výstuž môže určovať čistenie, aj keď ju na etikete vláknového zloženia nevidí samostatne.",
    ],
    "table1_heading": "Nažehľovací, všívací, tkaný, pletený a netkaný vlizelín",
    "table1_intro": "Kategórie sa môžu kombinovať: nažehľovací výrobok môže mať netkaný, tkaný aj pletený nosič. Tabuľka oddeľuje dve rôzne otázky.",
    "table1_headers": ["Typ", "Ako drží alebo vzniká", "Typické použitie", "Hlavné riziko"],
    "table1_rows": [
        ("Nažehľovací", "Termoplastické lepidlo sa aktivuje teplotou, tlakom a časom.", "Golier, manžeta, léga, pás, predný diel a drobné výstuže.", "Slabé spojenie, bubliny, presiaknutie lepidla a delaminácia."),
        ("Všívací", "Výstuž sa upevní stehmi alebo konštrukčne medzi vrstvy.", "Krajčírske saká, citlivé látky, tvarovanie a historické techniky.", "Posun vrstiev, zrážanie, deformácia stehom alebo nesprávnym sušením."),
        ("Netkaný nosič", "Vlákna sú spojené mechanicky, tepelne alebo chemicky bez klasickej väzby.", "Široké spektrum ľahkých a stredných výstuží.", "Smerová stabilita, zrážanie, starnutie spojiva a nevhodný výber hmotnosti."),
        ("Tkaný nosič", "Osnova a útok vytvárajú väzbu podobne ako pri bežnej tkanine.", "Košele, saká a miesta, kde sa sleduje smer nite.", "Rozdielne zrazenie, šikmé položenie a odtlačenie štruktúry."),
        ("Pletený nosič", "Očká dávajú pružnosť a prispôsobenie smeru.", "Úplety a pružné módne látky.", "Nesúlad pružnosti, zvlnenie a deformácia po teple alebo praní."),
    ],
    "sections": [
        {
            "heading": "Golier, manžeta a léga košele",
            "paragraphs": [
                "Košeľová výstuž musí držať tvar, ale zároveň umožniť ohyb a opakované pranie. Príliš tuhá výstuž môže vytvoriť ostrý prechod a príliš mäkká nedrží golier. Po cykle sa môže ukázať rozdielne zrazenie vrchnej bavlny a nosiča alebo slabé spojenie pri okraji. Krútiaca sa léga môže súvisieť aj so smerom strihu, nie iba s lepidlom.",
                "Pred praním zapnite alebo uvoľnite gombíky podľa návodu, vyberte odnímateľné výstuže goliera a škvrnu pri krku ošetrujte lokálne bez premočenia celej vystuženej plochy silným koncentrátom. Po praní košeľu vyrovnajte podľa švov. Pri žehlení netlačte dlhodobo na jednu bublinu v nádeji, že sa spoj automaticky obnoví.",
            ],
        },
        {
            "heading": "Pás nohavíc, sukne a spevnený okraj",
            "paragraphs": [
                "Pás spája vrchnú látku, výstuž, švy, zapínanie a často podšívku alebo protišmykovú pásku. Pot, tlak a ohyb sa sústreďujú na malej ploche. Ak sa vrstvy zrazia rozdielne, pás sa môže zvlniť alebo skrátiť voči telu odevu. Kovové háčiky a zips zároveň menia vhodnosť pracieho cyklu.",
                "Pri lokálnej škvrne najprv odsajte pot alebo mastnotu a produkt skúste na prídavku šva. Pás nekrúťte a mokré nohavice nevešajte tak, aby celá hmotnosť ťahala jedno pútko. Ak sa okraj po vyschnutí stáča, porovnajte dĺžku vrchnej a vnútornej vrstvy a pred ďalším cyklom požiadajte krajčíra o posúdenie.",
            ],
        },
        {
            "heading": "Sako, kabát a lepená predná časť",
            "paragraphs": [
                f"Predná časť saka môže mať plošne lepenú výstuž alebo zložitejší všívaný krajčírsky systém. Chlopňa, prieramok, rameno a hrudník pracujú pri nosení spolu. Návod <a href=\"{ARTICLE_BLAZER}\">ako prať sako doma a kedy ísť do čistiarne</a> zdôrazňuje, že vrchné vlákno nie je jediným kritériom. Profesionálny symbol má pri takomto odeve vysokú váhu.",
                "Ponorenie môže zmeniť tvar ramien, podšívku aj lepené rozhranie. Odstreďovanie vytvára lokálne záhyby a zavesenie mokrého saka na úzky vešiak ťahá predné diely. Ak výrobca domáce pranie nepovoľuje, jemný program nie je bezpečný kompromis. Povrchové osvieženie, vetranie a odborné čistenie sú odlišné úlohy.",
            ],
            "callout": {
                "title": "Prečo sa sako neposudzuje iba podľa vrchnej látky",
                "items": [
                    "Predný diel môže byť plošne lepený alebo vystužený viacerými všívanými vrstvami.",
                    "Podšívka, ramenná výplň a páska pri šve majú vlastné zrážanie.",
                    "Chlopňa a golier držia tvar žehlením a konštrukciou, ktorú mokrý cyklus môže zmeniť.",
                    "Profesionálne čistenie na etikete nie je odporúčanie, ktoré nahradí nízka teplota vody.",
                ],
                "background": "#f7fbf8",
                "border": "#dbe5de",
            },
        },
        {
            "heading": "Výstuž pri výšivke, aplikácii a kreatívnom šití",
            "paragraphs": [
                "Stabilizátor pod výšivkou obmedzuje sťahovanie stehov a môže byť odtrhávací, odstrihávací, rozpustný alebo trvalý. Nie každý patrí do hotového výrobku natrvalo. Zvyšok príliš tuhej výstuže môže škriabať pokožku, zatiaľ čo jej predčasné odstránenie deformuje motív. Pri praní rozhoduje typ nite, podkladu, lepidla aj nosnej látky.",
                "Vo vode rozpustný stabilizátor sa nemá zamieňať s bežným vlizelínom. Vlhkosť ho môže zámerne odstrániť, ale neúplný oplach zanechá tvrdý okraj. Samolepiaci alebo dočasne fixovaný materiál môže mať zvyškové lepidlo. Pri domácom výrobku uchovajte názov produktu a aplikačné údaje, aby neskoršia starostlivosť nestála na odhade.",
            ],
        },
        {
            "heading": "Ako sa výstuž fixuje teplotou, tlakom a časom",
            "paragraphs": [
                "Pri fixácii sa povrch lepiacich bodov zmäkčí, prenikne do štruktúry vrchnej látky a po ochladení vytvorí spoj. Teplota na displeji lisu alebo žehličky nie je automaticky teplota v rozhraní. Ovplyvňuje ju hrúbka látky, vlhkosť, ochranná vrstva, tlak a čas. Krátke posúvanie žehličky nemusí dať rovnomerný výsledok.",
                "Výrobcovia výstuže uvádzajú skúšobné parametre a odporúčajú test na konkrétnej látke. Po fixácii sa diel nechá vychladnúť naplocho bez pohybu, aby sa spoj stabilizoval. Pri šití doma nepoužívajte všeobecnú najvyššiu teplotu; termoplastické vlákno alebo povrch vrchnej látky sa môže poškodiť skôr, než vznikne dobré spojenie.",
            ],
        },
        {
            "heading": "Prečo vznikajú bubliny a delaminácia po praní",
            "paragraphs": [
                "Bublina vznikne, keď sa vrstvy lokálne oddelia alebo zmenia rozmer odlišne. Príčinou môže byť nízka pôvodná pevnosť spoja, kontaminácia pred fixáciou, nesprávne parametre, zrazenie jednej vrstvy, nevhodná chémia alebo opakovaný ohyb. Voda môže dočasne zvýrazniť zvlnenie, preto stav posudzujte po úplnom vysušení a ustálení.",
                "ASTM D2724 hodnotí vlastnosti lepených, fixovaných a laminovaných odevných textílií pred a po praní alebo chemickom čistení vrátane pevnosti spoja. Samotná existencia metódy neznamená, že každý odev bol podľa nej skúšaný. Pri reklamácii fotografujte bubliny pri šikmom svetle, zloženie, symboly a presný použitý cyklus.",
            ],
        },
        {
            "heading": "Presiaknutie lepidla a odtlačené body na líci",
            "paragraphs": [
                "Ak lepidlo prejde cez riedku alebo citlivú vrchnú látku, na líci sa objavia lesklé bodky, tvrdé miesta alebo zmena farby. Príčinou môže byť priveľa lepidla, nevhodný typ výstuže, vysoká teplota alebo tlak. Domáce rozpúšťadlo môže škvrnu rozšíriť a poškodiť vlákno aj farbivo.",
                "Odtlačenie štruktúry nemusí byť chemické presiaknutie. Hrubý nosič alebo okraj výstuže sa môže po silnom žehlení prekresliť na líc. Miesto pozorujte pri bočnom svetle a porovnajte omak. Ďalšie pritlačenie žehličky zvyčajne nepomôže; pri rozpracovanom diele výstuž vymeňte, pri hotovom odeve vyhľadajte krajčíra.",
            ],
        },
        {
            "heading": "Rozdielne zrazenie vrchnej látky a výstuže",
            "paragraphs": [
                f"Pred šitím sa materiály pripravujú podľa budúceho spôsobu údržby. Ak sa bavlnená vrchná látka zrazí a výstuž nie, plocha sa zvlní; opačný rozdiel môže líc stiahnuť. Mechanizmy zmeny rozmeru rozoberá článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie po praní zrazí</a>. Výsledok ovplyvňuje aj smer strihu a podšívka.",
                "Na hotovom odeve sa rozdiel nedá bezpečne opraviť násilným napínaním. Zmerajte symetrické diely, nechajte kus úplne vyschnúť a porovnajte švy. Pri novom výrobku ďalším horúcim praním nezisťujte, či sa vrstvy dorovnajú. Taký pokus môže poškodiť zdravú časť a sťažiť reklamáciu.",
            ],
        },
        {
            "heading": "Ako prať odev s vlizelínom bez poškodenia konštrukcie",
            "paragraphs": [
                "Riaďte sa etiketou hotového odevu. Pri povolenom praní zatvorte kovanie, vyprázdnite vrecká, uvoľnite odnímateľné prvky a oddeľte kus od ťažkých textílií. Použite primeranú náplň, dávku a mechaniku. Koncentrovaný produkt nenalievajte priamo na vystužený golier alebo chlopňu, pokiaľ to výrobca výslovne neuvádza.",
                "Po cykle odev ihneď vyberte, podoprite a vytvarujte podľa švov bez ťahania. Košeľu možno sušiť inak než sako; symbol rozhoduje. Vystuženú plochu neskrúcajte a nevkladajte do sušičky bez povolenia. Keď sa objaví bublina, nechajte ju ochladnúť a vyschnúť a stav fotografujte namiesto opakovaného tepelného zásahu.",
            ],
        },
        {
            "heading": "Žehlenie vystuženého goliera, légy a chlopne",
            "paragraphs": [
                f"Pri bežnej prateľnej košeli žehlite podľa symbolu a zloženia, z rubu alebo cez ochrannú tkaninu podľa povrchu. Praktické základy uvádza návod <a href=\"{ARTICLE_IRONING}\">ako vyžehliť košeľu</a>. Žehličku neprikladajte dlho na jedno miesto a vystuženú zónu neohýbajte, kým je lepidlo teplé.",
                "Para dodáva vodu aj teplo a môže dočasne zmäkčiť spoj. Pri saku sa tvar chlopne vytvára priestorovo; sploštenie na tvrdej doske ho môže pokaziť aj bez delaminácie. Ak neviete, aký systém je vo vnútri, zvoľte nižší povolený zásah alebo profesionálne lisovanie. Domáca žehlička nie je fixačný lis s kontrolovaným tlakom.",
            ],
        },
        {
            "heading": "Oprava odlepeného vlizelínu a hranice domáceho zásahu",
            "paragraphs": [
                "Pri rozpracovanom šití možno diel často rozobrať a výstuž vymeniť podľa pokynov výrobcu. Pri hotovom odeve je prístup k rozhraniu obmedzený. Opätovné prežehlenie cez líc môže krátko zmeniť vzhľad, ale bez odstránenia príčiny nemusí vytvoriť rovnomernú pevnosť a môže lepidlo pretlačiť.",
                "Krajčír posúdi, či sa dá otvoriť podšívka, vložiť nová výstuž alebo tvar stabilizovať švom. Pri lacnom odeve môže byť oprava neúmerná, no pri kvalitnom saku alebo sentimentálnom kuse má zmysel. Domáce vstrekovanie lepidla cez látku vytvára tvrdé body a znemožňuje čistú neskoršiu opravu.",
            ],
        },
        {
            "heading": "Ako vybrať vlizelín pri šití a predísť neskoršiemu problému",
            "paragraphs": [
                "Výstuž vyberajte podľa hmotnosti, pružnosti, smeru, farby, povrchu a budúceho čistenia vrchnej látky. Jemná blúzka potrebuje iný omak než pás nohavíc. Vyrobte skúšobný sendvič dostatočne veľký na posúdenie ohybu a nechajte ho vychladnúť. Potom ho ošetrite rovnakým spôsobom, aký bude mať hotový výrobok.",
                "Zaznamenajte názov produktu, šaržu, teplotu, čas, tlak a či bola použitá para alebo ochranná tkanina. Malá vzorka ukáže presiaknutie, zmenu farby a prvotnú priľnavosť. Pri odevnej výrobe sa pevnosť spoja overuje systematicky; pri domácom šití aspoň uchovajte kontrolnú vzorku, aby ste po praní vedeli porovnať zmenu bez rozoberania odevu.",
            ],
        },
    ],
    "table2_heading": "Porucha vystuženej časti: čo môže znamenať",
    "table2_intro": "Záhyb, rozdielne zrazenie, odtlačenie a delaminácia vyzerajú podobne iba na prvý pohľad. Stav posúďte po úplnom vysušení a ochladení.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Okrúhle alebo nepravidelné bubliny", "Slabý spoj, kontaminácia, rozdielne zrazenie alebo degradácia lepidla.", "Rozsah pri bočnom svetle, štítok a použitý cyklus.", "Nezohrievať naslepo; zdokumentovať a konzultovať opravu."),
        ("Pravidelné bodky na líci", "Presiaknutie lepidla alebo odtlačenie jeho nánosu.", "Lesk, tvrdosť a zhodu so vzorom lepidla.", "Nepoužívať rozpúšťadlo; odborné posúdenie."),
        ("Zvlnený okraj alebo léga", "Rozdielne zrazenie, smer strihu alebo napätie šva.", "Symetriu, dĺžku vrstiev a stav stehu.", "Nenapínať; krajčírsky rozbor pred ďalším praním."),
        ("Vrstva sa oddeľuje od okraja", "Nedostatočná pôvodná fixácia alebo starnutie spoja.", "Či sa porucha šíri a či je lepidlo lepkavé.", "Zastaviť teplo a mechaniku, riešiť opravu."),
        ("Golier je po praní príliš tvrdý", "Zvyšok produktu, nevhodná výstuž alebo zmena spojiva.", "Oplach, rovnomernosť a porovnanie so skrytým miestom.", "Pri povolení šetrne opláchnuť; pri lokálnej tvrdosti neexperimentovať."),
    ],
    "steps_heading": "Ako ošetriť odev s výstužou krok za krokom",
    "steps": [
        "Prečítajte štítok celého odevu a nájdite vystužené zóny, podšívku, ramenné diely a lepené okraje.",
        "Pri bočnom svetle zdokumentujte existujúce bubliny, vlny, presiaknutie a rozdielny tvar vrstiev.",
        "Škvrnu ošetrite kompatibilne a bez veľkého premočenia vystuženej plochy koncentrovaným produktom.",
        "Ak je pranie povolené, zvoľte predpísanú teplotu, mechaniku, dávku a primeranú náplň bez ťažkých kusov.",
        "Mokrý odev vyberte s oporou, nekrúťte ho a vytvarujte golier, légu, pás a chlopňu podľa švov.",
        "Sušte spôsobom zo štítku a nepoužívajte radiátor, fén ani sušičku bez výslovného povolenia.",
        "Žehlite až podľa symbolu, krátko a bez sústredeného tlaku na bublinu alebo lepený okraj.",
        "Po úplnom vyschnutí a ochladení porovnajte stav; rastúcu delamináciu riešte s krajčírom alebo čistiarňou.",
    ],
    "remember": [
        "Je výstuž nažehľovacia alebo všívacia a aký má nosič?",
        "Ktoré časti odevu sú vystužené a ktorá vrstva má najnižší limit?",
        "Povoľuje etiketa vodu, strojové pranie, sušičku, paru alebo iba profesionálne čistenie?",
        "Sú bubliny viditeľné už pred zásahom a menia sa po úplnom vysušení?",
        "Môže sa vrchná látka a výstuž zraziť alebo natiahnuť odlišne?",
        "Má mokrý odev pri prenášaní a sušení dostatočnú tvarovú oporu?",
    ],
    "mistakes": [
        "Považovať vlizelín za jedno vlákno a ignorovať nosič, lepidlo a vrchnú látku.",
        "Vyprať lepené sako podľa programu vhodného iba pre jeho povrchovú vlnu alebo polyester.",
        "Tlačiť horúcou žehličkou na bublinu bez znalosti fixačných parametrov.",
        "Použiť rozpúšťadlo na presiaknuté lepidlo a rozšíriť poškodenie líca.",
        "Vešať ťažký mokrý vystužený odev na úzky vešiak.",
        "Zameniť dočasné pokrčenie za delamináciu alebo skutočnú delamináciu za bežný záhyb.",
    ],
    "expert_heading": "Odbornejší pohľad: pevnosť spoja, kompatibilita vrstiev a skúšanie",
    "expert": [
        "Pevnosť lepeného spoja závisí od zmáčania povrchu roztaveným polymérom, preniknutia do štruktúry a súdržnosti po ochladení. Príliš malá väzba zlyhá odlepením, priveľký prienik zmení omak alebo sa ukáže na líci. Povrchová úprava vrchnej látky môže priľnavosť znížiť, preto technický list výstuže nikdy nenahrádza skúšku s konkrétnou látkou.",
        "ASTM D2724 zahŕňa hodnotenie lepených, laminovaných a fixovaných odevných textílií po definovanom praní alebo chemickom čistení. Výsledok treba čítať s podmienkami, druhom spoja a požadovanou pevnosťou. AATCC TM135 zas sleduje rozmerové zmeny po štandardizovaných domácich postupoch. Kombinácia údajov pomáha odhaliť nesúlad, ale etiketu hotového odevu stanovuje výrobca na základe celku.",
        "ISO 9092:2026 upravuje slovnú zásobu netkaných textílií. Je dôležitá pri presnom pomenovaní nosiča, no interlining ako funkčná vrstva môže využívať aj tkaný alebo pletený materiál. Spotrebiteľské slovo vlizelín preto nestačí na materiálovú diagnózu. Pri oprave treba určiť funkciu, smer, pružnosť, hmotnosť a spôsob spojenia.",
    ],
    "source_intro": "Zdroje oddeľujú netkanú konštrukciu od funkcie interliningu a opisujú aplikačné parametre, ošetrovanie a skúšanie lepených vrstiev. Presný postup určuje výrobca výstuže a hotového odevu.",
    "sources": [
        ("ISO 9092:2026: slovná zásoba netkaných textílií", ISO_NONWOVEN),
        ("Freudenberg/Vlieseline: prehľad výstužných materiálov", VLIESELINE_BROCHURE),
        ("Freudenberg: funkcia a lepenie interliningu", VLIESELINE_FUNCTION),
        ("Vlieseline H 180: príklad parametrov a starostlivosti", VLIESELINE_CARE),
        ("ASTM D2724: lepené, fixované a laminované odevné textílie", ASTM_INTERLINING),
        ("AATCC TM135: rozmerové zmeny po domácom praní", AATCC_DIMENSION),
        ("GINETEX: systém symbolov ošetrovania", GINETEX),
    ],
    "product_heading": "Prací gél patrí iba k odevu, ktorý má povolené domáce pranie",
    "product_intro": "Pri prateľnej košeli alebo inom odeve s výstužou možno vybrať gél podľa zloženia a farby, ale etiketa celého výrobku musí domáci cyklus výslovne povoľovať.",
    "product_name": LAUNDRY_PRODUCT_NAME,
    "product_url": LAUNDRY_PRODUCT_URL,
    "product_text": "Tekutý gél umožňuje presné dávkovanie podľa tvrdosti vody a náplne. Nepoužívajte koncentrovaný produkt priamo na lepenú plochu bez pokynu a dbajte na dôkladný povolený oplach.",
    "product_limit": "Produkt nie je riešením delaminácie a neprepisuje symbol profesionálneho čistenia. Lepené sako, kabát alebo citlivý odev neperte iba preto, že vrchné vlákno by vodu znieslo.",
    "category_heading": "Prací prostriedok vyberte až po posúdení celej konštrukcie",
    "category_intro": "Pri bežnej prateľnej bielizni rozhoduje vlákno, farba a znečistenie. Pri vystuženom odeve k nim vždy pridajte lepidlo, podšívku, tvar a profesionálny symbol.",
    "category_name": LAUNDRY_CATEGORY_NAME,
    "category_url": LAUNDRY_CATEGORY_URL,
    "category_text": "V kategórii môžete porovnať pracie gély pre rôzne druhy prateľnej bielizne. Pred výberom potvrďte, že celý vystužený odev smie ísť do vody a zvoleného cyklu.",
    "related": [
        ("Ako prať sako doma a kedy ísť do čistiarne", ARTICLE_BLAZER),
        ("Ako správne vyžehliť košeľu", ARTICLE_IRONING),
        ("Ako čítať materiálový a ošetrovací štítok", ARTICLE_LABEL),
        ("Prečo sa oblečenie po praní zráža", ARTICLE_SHRINKAGE),
        ("Ako odstrániť rôzne škvrny z oblečenia", ARTICLE_STAIN),
        ("Prečo farby pri praní a trení blednú", ARTICLE_COLOR),
    ],
    "faq_title": "vlizelín, výstuž a lepené časti odevu",
    "faq": [
        ("Čo je vlizelín?", "Je to bežné pomenovanie výstužnej medzivrstvy v odevoch a pri šití. Môže byť nažehľovacia alebo všívacia a mať netkaný, tkaný či pletený nosič."),
        ("Je každý vlizelín netkaná textília?", "Nie. Netkaný nosič je častý, ale existujú aj tkané a pletené výstuže."),
        ("Čo znamená nažehľovací vlizelín?", "Na povrchu má lepidlo, ktoré sa aktivuje určenou kombináciou teploty, tlaku a času. Neznamená to neobmedzenú odolnosť pri neskoršom žehlení."),
        ("Prečo sa po praní vytvorili bubliny?", "Mohlo dôjsť k delaminácii, slabému pôvodnému spojeniu alebo rozdielnemu zrazeniu vrstiev. Stav posúďte po vysušení a nepridávajte teplo naslepo."),
        ("Dá sa odlepený vlizelín znovu nažehliť?", "Niekedy pri rozpracovanom diele podľa technického listu, ale pri hotovom odeve môže teplo vytlačiť lepidlo alebo poškodiť líc. Vhodnejšie je krajčírske posúdenie."),
        ("Môže ísť sako s vlizelínom do práčky?", "Iba ak to povoľuje etiketa celého saka. Vrchné vláknové zloženie samo o sebe nestačí."),
        ("Ako žehliť vystužený golier?", "Podľa symbolu odevu, krátko a pri primeranej teplote, bez dlhého tlaku na jedno miesto. Teplú lepenú zónu nechajte ochladnúť bez ohýbania."),
        ("Čo je presiaknutie lepidla?", "Lepidlo prejde alebo sa odtlačí na líc a vytvorí lesklé, tvrdé či bodkované miesto. Nepoužívajte naň náhodné rozpúšťadlo."),
        ("Ako vybrať výstuž pri šití?", "Podľa hmotnosti, pružnosti, smeru, farby, povrchu a budúceho čistenia vrchnej látky. Vždy vytvorte a ošetrite skúšobnú vzorku."),
    ],
}


ARTICLES: list[dict[str, object]] = [DAMASK, KAPOK, SISAL, INTERLINING]


def preflight_links(articles: list[dict[str, object]]) -> dict[str, object]:
    target_urls = {f"{BASE}/n/{article['link']}" for article in articles}
    outgoing_urls = {
        urljoin(BASE, href) if href.startswith("/") else href
        for article in articles
        for href in article_hrefs(str(article["long"]))
    }
    with ThreadPoolExecutor(max_workers=6) as executor:
        checks = list(executor.map(fetch_status, sorted(target_urls | outgoing_urls)))
    for check in checks:
        if check["url"] in target_urls:
            check["expected_status"] = 404
            check["ok"] = check["status"] == 404
    report = {
        "batch": "batch-49",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "target_count": len(target_urls),
        "outgoing_count": len(outgoing_urls),
        "check_count": len(checks),
        "failure_count": sum(not check["ok"] for check in checks),
        "checks": checks,
    }
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def seven_word_shingles(value: str) -> set[tuple[str, ...]]:
    words = [word.casefold() for word in WORD_RE.findall(value)]
    return {tuple(words[index : index + 7]) for index in range(max(0, len(words) - 6))}


def jaccard(left: set[tuple[str, ...]], right: set[tuple[str, ...]]) -> float:
    if not left and not right:
        return 1.0
    return len(left & right) / len(left | right)


def main() -> None:
    candidate_titles = [
        line.strip()
        for line in CANDIDATES.read_text(encoding="utf-8-sig").splitlines()
        if line.strip()
    ]
    article_by_title = {str(article["title"]): article for article in ARTICLES}
    if len(article_by_title) != len(ARTICLES) or set(candidate_titles) != set(article_by_title):
        raise SystemExit("Candidate titles and article definitions do not match exactly")
    slugs = [str(article["link"]) for article in ARTICLES]
    if len(slugs) != len(set(slugs)):
        raise SystemExit("Batch contains duplicate slugs")

    rendered: list[dict[str, object]] = []
    metrics: list[dict[str, object]] = []
    for article in ARTICLES:
        body = render_article(article)
        public_text = f"{article['title']} {article['short']} {body}"
        visible = visible_text(body)
        if FORBIDDEN_PUBLIC_RE.search(public_text):
            raise SystemExit(f"Forbidden public wording in {article['title']}")
        if FIXED_PRICE_RE.search(visible_text(public_text)):
            raise SystemExit(f"Fixed price found in {article['title']}")
        one_character_paragraphs = [
            visible_text(value).strip()
            for value in re.findall(r"<p\b[^>]*>(.*?)</p>", body, flags=re.IGNORECASE | re.DOTALL)
            if len(visible_text(value).strip()) == 1
        ]
        metric = {
            "title": article["title"],
            "slug": article["link"],
            "words": len(WORD_RE.findall(visible)),
            "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
            "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
            "responsive_tables": len(
                re.findall(r'<div\b[^>]*style="[^"]*overflow-x:\s*auto', body, re.IGNORECASE)
            ),
            "styled_blocks": len(re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)),
            "action_buttons": len(
                re.findall(r'<a\b[^>]*style="[^"]*display:\s*inline-block', body, re.IGNORECASE)
            ),
            "one_character_paragraphs": len(one_character_paragraphs),
        }
        if metric["words"] < 2800:
            raise SystemExit(f"Article is too short: {article['title']} ({metric['words']} words)")
        if metric["h2"] < 24 or metric["tables"] < 2 or metric["responsive_tables"] != metric["tables"]:
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if metric["styled_blocks"] < 10 or metric["action_buttons"] < 2 or metric["one_character_paragraphs"]:
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": body,
                "link": article["link"],
                "date_posted": PUBLISH_DATE,
                "time_posted": "15:00:00",
                "commenting": False,
                "title_tag": article["title"],
                "description": article["meta"],
            }
        )

    overlaps: list[dict[str, object]] = []
    for index, left in enumerate(rendered):
        for right in rendered[index + 1 :]:
            score = jaccard(
                seven_word_shingles(visible_text(str(left["long"]))),
                seven_word_shingles(visible_text(str(right["long"]))),
            )
            overlaps.append({"left": left["title"], "right": right["title"], "score": round(score, 4)})
            if score >= 0.13:
                raise SystemExit(f"Article bodies overlap too much: {left['title']} / {right['title']} ({score:.4f})")

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(rendered, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 49 link preflight failed")
    print(
        json.dumps(
            {
                "article_count": len(rendered),
                "metrics": metrics,
                "seven_word_shingle_overlaps": overlaps,
                "link_preflight": True,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

import argparse
import json
import re
import textwrap
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[3]
CONFIG_PATH = Path.home() / ".codex" / "config.toml"
OUT_JSON = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-17-mixed-stains-travel-five-2026-06-16.json"
MCP_RESULTS = ROOT / "content" / "VEVO_CONTENT" / "exports" / "retrofit-wave-17-mixed-stains-travel-five-2026-06-16-mcp-results.json"


ARTICLES = [
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-17-2026-06-10-articles.json",
        "slug": "ako-odstranit-lak-na-vlasy-z-goliera-kosele-a-satky",
        "post_id": "2180",
        "url": "https://www.vevo.sk/n/ako-odstranit-lak-na-vlasy-z-goliera-kosele-a-satky",
        "topic": "hair_spray",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-15-2026-06-09-articles.json",
        "slug": "ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku",
        "post_id": "2137",
        "url": "https://www.vevo.sk/n/ako-odstranit-majonezu-a-dressing-z-obrusu-bez-mastneho-flaku",
        "topic": "mayo_dressing",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-odstranit-hrdzave-flaky-od-stipcov-a-snury-na-bielizen",
        "post_id": "2224",
        "url": "https://www.vevo.sk/n/ako-odstranit-hrdzave-flaky-od-stipcov-a-snury-na-bielizen",
        "topic": "rust_pins",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-16-2026-06-10-articles.json",
        "slug": "ako-odstranit-lak-na-nechty-z-textilu-bez-rozmazania-skvrny",
        "post_id": "2145",
        "url": "https://www.vevo.sk/n/ako-odstranit-lak-na-nechty-z-textilu-bez-rozmazania-skvrny",
        "topic": "nail_polish",
    },
    {
        "source": ROOT / "content" / "VEVO_CONTENT" / "imports" / "batch-19-2026-06-10-articles.json",
        "slug": "ako-prat-cestovne-oblecenie-po-dlhom-lete-alebo-vlaku",
        "post_id": "2216",
        "url": "https://www.vevo.sk/n/ako-prat-cestovne-oblecenie-po-dlhom-lete-alebo-vlaku",
        "topic": "travel_clothes",
    },
]


def clean(markup):
    return textwrap.dedent(markup).strip()


def table(headers, rows):
    head = "".join(f'<th style="border: 1px solid #e5e5e5; padding: 10px; text-align: left;">{h}</th>' for h in headers)
    body = "\n".join(
        "<tr>" + "".join(f'<td style="border: 1px solid #e5e5e5; padding: 10px;">{c}</td>' for c in row) + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{head}</tr></thead>\n<tbody>\n{body}\n</tbody>\n</table>"
    )


def note_card(title, bullets):
    items = "".join(f"<li>{item}</li>" for item in bullets)
    return clean(
        f"""
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">{title}</h2>
        <ul>{items}</ul>
        </div>
        """
    )


def recommendation_card(config):
    return clean(
        f"""
        <div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
        <h2 style="margin-top: 0;">Odporúčané riešenie pre pranie po predčistení</h2>
        <p>{config["recommendation_intro"]}</p>
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 14px; margin: 14px 0;">
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
        <p>{config["product_text"]}</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
        </div>
        <div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff;">
        <h3 style="margin-top: 0;">Kategória pracie gély</h3>
        <p>Ak riešite škvrny, pach alebo opakované pranie textílií v domácnosti, oplatí sa mať doma šetrný prací gél a používať ho až po rozumnom predčistení problému.</p>
        <p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
        </div>
        </div>
        </div>
        """
    )


def build_related_links(links):
    items = "\n".join(f'<li><a href="{href}">{label}</a></li>' for href, label in links)
    items += '\n<li><a href="/n/ako-vybrat-praci-gel-podla-typu-bielizne">Ako vybrať prací gél podľa typu bielizne</a></li>'
    return f"<h2>Súvisiace návody na VEVO</h2>\n<ul>\n{items}\n</ul>"


TOPICS = {
    "hair_spray": {
        "marker": "Detailnejší postup na lak na vlasy, stylingový film a golier košele",
        "problem": "lak na vlasy na textile nie je len vôňa, ale najmä tenký stylingový film, ktorý môže golier spevniť, zlepiť vlákna a držať prach",
        "main_textile": "golieri košele, šatke, blúzke, mikine a tmavom tričku",
        "avoid": "žehlenie alebo sušičku pred kontrolou, pretože teplo môže film zafixovať do vlákien",
        "diagnosis": [
            "<strong>Skontrolujte dotyk:</strong> lak môže byť viditeľný málo, ale látka je tuhá alebo lepkavá.",
            "<strong>Najviac trpí golier:</strong> mieša sa tam lak, parfum, pot a maz z pokožky.",
            "<strong>Šatka potrebuje jemnosť:</strong> tenká látka sa pri drhnutí ľahko vytiahne.",
            "<strong>Najprv film, potom vôňa:</strong> prevoňanie nevyrieši vrstvu stylingu.",
        ],
        "state_rows": [
            ("čerstvý aerosól", "vyvetrať a jemne predčistiť miesto", "netrieť nasucho silou"),
            ("tuhý golier", "uvoľniť film pred praním", "kontrolovať aj rub"),
            ("mastno-lepkavý okraj", "prať až po lokálnom ošetrení", "často sa mieša s potom"),
            ("jemná šatka", "test na skrytom mieste", "pozor na zmenu tvaru"),
        ],
        "textile_rows": [
            ("košeľa", "predčistiť golier a manžety", "film sa drží na namáhaných miestach"),
            ("šatka", "pracovať po malých plochách", "jemná väzba neznáša kefku"),
            ("mikina", "pred praním skontrolovať kapucňu a lem", "lak sa prenáša z vlasov"),
            ("tmavé tričko", "prať naruby a dobre opláchnuť", "zvyšky môžu vytvoriť mapu"),
        ],
        "sections": [
            ("Ako odstrániť lak na vlasy z goliera košele", "Golier najprv prezrite pri dennom svetle a prejdite prstami. Ak je miesto tvrdšie alebo lepkavé, nejde iba o zápach. Jemne ho navlhčite, nechajte povrch povoliť a až potom použite malé množstvo pracieho gélu na lokálne predčistenie.", "Košeľu perte podľa štítku a pred sušením skontrolujte, či golier nezostal tuhý. Ak áno, postup zopakujte skôr, než sa zvyšok zafixuje teplom."),
            ("Lak na vlasy na šatke", "Šatka býva často z viskózy, polyesteru, hodvábu alebo zmesi. Pri takýchto materiáloch je dôležitejšia trpezlivosť než sila. Netrite jeden bod dlho, nekrúťte látku a najprv si overte, či materiál nepúšťa farbu.", "Ak je šatka drahšia alebo má zložitú väzbu, domáce čistenie držte veľmi mierne. Cieľom je uvoľniť film bez vytiahnutia vlákien."),
            ("Lak, parfum a pot na jednom mieste", "Na golieri sa často stretnú tri vrstvy: stylingový lak z vlasov, parfum a pot. Preto sa škvrna môže tváriť ako zápach, hoci problémom je produktový nános. Najprv riešte fyzickú vrstvu na látke, až potom sviežosť.", "Súvisiacu tému rozoberá článok <a href=\"/n/ako-prat-oblecenie-po-kadernictve-od-vlasov-farby-a-lakov\">ako prať oblečenie po kaderníctve</a>."),
            ("Kedy nepomôže iba dlhší program", "Dlhší prací program nepomôže, ak je film na texte zle uvoľnený alebo sa kus perie v preplnenej práčke. Pri golieroch je lepší krátky lokálny zásah a normálne pranie v dávke, ktorá má priestor na pohyb.", "Ak sa zvyšky stylingu opakujú, kontrolujte aj dávkovanie gélu a oplach. Priveľa pracieho prostriedku môže na tuhom mieste zanechať ďalší povlak."),
            ("Ako predchádzať lakovým mapám", "Pri používaní laku nechajte produkt krátko uschnúť pred obliekaním šatky alebo kabáta. Pri košeliach pomáha rýchla kontrola goliera pred košom na bielizeň, nie až v deň prania.", "Ak si lak aplikujete denne, oblečenie s kontaktom pri vlasoch neodkladajte vlhké alebo prevoňané do skrine. Produktový film sa potom horšie odstraňuje."),
        ],
        "depth": [
            ("Prečo lak spevňuje aj látku", "Stylingový lak je navrhnutý na vytvorenie filmu na vlasoch. Keď sa dostane na textil, môže sa správať podobne: spevní povrch, zachytí prach a pri vlhkosti sa môže zmeniť na lepkavú mapu.", "Preto je dôležité hodnotiť nielen farbu škvrny, ale aj dotyk. Tuhosť je pri laku často lepší signál než samotný fľak."),
            ("Čo sledovať po druhom praní", "Ak golier po druhom praní stále pôsobí tuhšie, možno na ňom už nie je špina, ale zmena povrchu spôsobená trením alebo teplom. Vtedy nepomáha ďalšie drhnutie, ale šetrnejší režim prania do budúcnosti.", "Pri opakovanom probléme si vytvorte zvyk predčistiť golier pred každým praním košieľ, podobne ako pri make-upe alebo parfume."),
        ],
        "expert_title": "Odbornejší pohľad: filmotvorné produkty a prečo ich nestačí iba prevoňať",
        "expert_p1": "Stylingové produkty často fungujú tak, že na povrchu vytvoria tenkú vrstvu. Na vlasoch je to žiadaný efekt, na textile však môže vrstva držať prach, pach a ďalšie nečistoty. Pri praní preto rozhoduje lokálne uvoľnenie vrstvy a dostatočný oplach.",
        "rule": "Pri laku na vlasy sledujte najmä tuhosť a lepivosť. Ak látka nepôsobí prirodzene mäkko, nežehlite ju a nesušte horúco, kým miesto ešte raz neskontrolujete.",
        "recommendation_intro": "Pri laku na vlasy dáva zmysel najprv uvoľniť stylingový film na golieri alebo šatke. Až potom má prací gél pri bežnom praní priestor odstrániť zvyšky produktu a pach.",
        "product_text": "Vhodný na následné pranie košieľ, tričiek a bežných textílií po lokálnom predčistení stylingového filmu. Pri šatkách a jemných látkach vždy rozhoduje štítok.",
        "links": [
            ("/n/ako-prat-oblecenie-po-kadernictve-od-vlasov-farby-a-lakov", "Ako prať oblečenie po kaderníctve od vlasov, farby a lakov"),
            ("/n/ako-odstranit-vlasove-serum-z-uteraka-a-goliera-kosele", "Ako odstrániť vlasové sérum z uteráka a goliera košele"),
            ("/n/ako-odstranit-podkladovy-krem-z-goliera-bluzky-a-kosele", "Ako odstrániť podkladový krém z goliera blúzky a košele"),
        ],
        "faq": [
            ("Prečo je golier po praní stále tuhý?", "Na látke môže zostať film z laku alebo zmes laku, potu a pracieho prostriedku. Miesto pred ďalším sušením lokálne ošetrite."),
            ("Môžem šatku s lakom drhnúť kefkou?", "Pri jemnej šatke radšej nie. Testujte na skrytom mieste a pracujte jemným pritláčaním, nie silným trením."),
            ("Pomôže aviváž?", "Aviváž nemá nahradiť odstránenie stylingového filmu. Použite ju až vtedy, keď je textil skutočne čistý a štítok ju povoľuje."),
        ],
    },
    "mayo_dressing": {
        "marker": "Detailnejší postup na majonézu, dressing a mastný fľak na obruse",
        "problem": "majonéza a dressing sú kombináciou tuku, vody, bielkovín, korenín a niekedy aj farbív, takže z obrusu sa neodstraňujú ako obyčajná mokrá škvrna",
        "main_textile": "bavlnenom obruse, látkovej servítke, prestieraní a kuchynskej utierke",
        "avoid": "horúcu vodu na začiatku a sušičku pred kontrolou mastného tieňa",
        "diagnosis": [
            "<strong>Najprv odobrať objem:</strong> majonézu nevtláčajte do väzby obrusu.",
            "<strong>Mastnota sa vracia:</strong> po praní môže zostať priesvitná mapa.",
            "<strong>Dressing môže farbiť:</strong> horčica, paprika alebo bylinky pridávajú pigment.",
            "<strong>Obrus kontrolujte pred žehlením:</strong> teplo zafixuje zvyšky tuku.",
        ],
        "state_rows": [
            ("čerstvá majonéza", "odobrať tupou hranou", "bez rozmazania do strán"),
            ("mastný tieň", "predčistiť gélom", "kontrola pred sušením"),
            ("farebný dressing", "riešiť aj pigment", "pozor na horčicu a papriku"),
            ("starší fľak", "opakovať mierny postup", "nezažehliť"),
        ],
        "textile_rows": [
            ("bavlnený obrus", "lokálne predčistiť a prať podľa štítku", "znáša viac, ale drží tuk"),
            ("ľanový obrus", "jemnejšie trenie a tvarovanie pri sušení", "ľan sa krčí a môže tvrdnúť"),
            ("látková servítka", "pracovať od okraja ku stredu", "škvrna býva pri ústach a jedle"),
            ("prestieranie", "skontrolovať podklad a švy", "mastnota sa drží v lemoch"),
        ],
        "sections": [
            ("Ako odstrániť majonézu z obrusu", "Prebytok najprv opatrne odoberte lyžičkou alebo tupou hranou. Netrite obrúsok do strán, pretože tuk sa vtlačí hlbšie do väzby. Potom miesto ošetrite malým množstvom pracieho gélu a nechajte krátko pôsobiť.", "Obrus perte podľa štítku a pred sušením skontrolujte mastný tieň. Najmä biely obrus môže vyzerať mokrý čistý, no po vyschnutí sa mastnota ukáže znovu."),
            ("Dressing s horčicou, paprikou alebo bylinkami", "Dressing často nie je iba tuk. Horčica, paprika, paradajka alebo bylinky pridajú farebnú zložku, ktorá potrebuje jemnejší a presnejší postup. Najprv riešte mastnotu, potom skontrolujte farebný zvyšok.", "Ak je škvrna žltá alebo oranžová, pomôže nadviazať na postupy pre <a href=\"/n/ako-odstranit-skvrny-od-horcice-z-tricka-obrusu-a-utierky\">horčicu</a> alebo <a href=\"/n/ako-odstranit-cervenu-papriku-z-tricka-a-kuchynskej-utierky\">červenú papriku</a>."),
            ("Prečo obrus nežehliť hneď po praní", "Žehlenie je pri obruse bežné, ale pri mastnom fľaku je rizikové. Ak v látke zostal tuk, teplo ho môže zafixovať a ďalšie odstraňovanie bude ťažšie. Najprv skontrolujte obrus pri dennom svetle.", "Ak vidíte priesvitnú mapu, radšej zopakujte predčistenie. Žehlenie patrí až na čistý obrus."),
            ("Majonéza na látkovej servítke", "Servítka býva menšia, ale škvrna je často koncentrovaná. Pracujte od okraja ku stredu a dávajte pozor, aby sa tuk nepreniesol na druhú stranu. Pomôže savá podložka pod škvrnou.", "Pri opakovanom používaní látkových servítok sa oplatí mať rutinu z článku <a href=\"/n/ako-prat-latkove-obrusky-a-prestieranie\">ako prať látkové obrúsky a prestieranie</a>."),
            ("Čo robiť po oslave", "Po väčšej oslave nenechajte obrus s mastnými škvrnami dlho v koši. Najprv označte alebo vytrieďte miesta s majonézou, olejom a omáčkami. Ak ich dáte do práčky bez kontroly, časť z nich sa iba rozšíri alebo ostane ako mapa.", "Pri viacerých škvrnách pomáha postupovať podľa najťažšej škvrny, nie podľa najľahšej."),
        ],
        "depth": [
            ("Mastnota a bielkoviny v jednej škvrne", "Majonéza obsahuje tuk a zložky, ktoré sa pri vysokej teplote môžu správať nevýhodne. Preto je rozumnejšie začať mierne, odstrániť objem a použiť lokálne predčistenie pred praním.", "Ak začnete horúcou vodou alebo sušičkou, môžete si pridať problém, ktorý už nebude iba mastný."),
            ("Ako rozlíšiť mastnú mapu od mokrého miesta", "Mokrá látka môže klamať. Po praní nechajte kontrolované miesto krátko preschnúť na vzduchu a pozrite sa naň zboku. Mastnota často vyzerá ako jemne tmavšia alebo priesvitnejšia plocha.", "Ak máte pochybnosť, nežehlite. Pri obrusoch je kontrola pred teplom najdôležitejšia časť celého postupu."),
        ],
        "expert_title": "Odbornejší pohľad: prečo mastné škvrny potrebujú najprv mechanické odobratie",
        "expert_p1": "Pri mastných jedlách je prvý krok často rozhodujúci. Ak sa objem škvrny vtlačí do väzby obrusu, prací kúpeľ musí riešiť omnoho väčšiu plochu. Mechanické odobratie prebytku preto nie je detail, ale prevencia rozšírenia škvrny.",
        "rule": "Pri majonéze platí: najprv odobrať objem, potom predčistiť mastnú časť, až potom prať celý obrus. Žehlenie patrí až po kontrole výsledku.",
        "recommendation_intro": "Pri mastných škvrnách od majonézy a dressingu je prací gél užitočný najmä po tom, čo z obrusu odstránite objem omáčky a lokálne uvoľníte tuk.",
        "product_text": "Vhodný na následné pranie obrusov, servítok a kuchynských textílií po lokálnom predčistení mastnej škvrny. Pri ľane sledujte štítok a sušte šetrne.",
        "links": [
            ("/n/ako-prat-obrus-po-oslave-aby-nezostali-mastne-skvrny-a-pachy", "Ako prať obrus po oslave, aby nezostali mastné škvrny a pachy"),
            ("/n/ako-prat-latkove-obrusky-a-prestieranie", "Ako prať látkové obrúsky a prestieranie"),
            ("/n/ako-odstranit-arasidove-maslo-z-tricka-obrusu-a-detskej-mikiny", "Ako odstrániť arašidové maslo z trička, obrusu a detskej mikiny"),
        ],
        "faq": [
            ("Môžem obrus s majonézou prať hneď?", "Najprv odoberte prebytok a lokálne uvoľnite mastnotu. Priamy prací cyklus môže škvrnu rozšíriť."),
            ("Prečo ostala po praní priesvitná mapa?", "V látke zostal tuk. Miesto nežehlite, predčistite ho znova a perte podľa štítku."),
            ("Je horúca voda dobrý nápad?", "Na začiatku radšej nie. Pri kombinácii tuku a ďalších zložiek začnite miernejšie a teplo použite až podľa štítku a po predčistení."),
        ],
    },
    "rust_pins": {
        "marker": "Detailnejší postup na hrdzavé fľaky od štipcov, šnúry a sušiaka",
        "problem": "hrdzavé fľaky od štipcov alebo šnúry nie sú bežná špina, ale kovové zafarbenie, ktoré sa pri nesprávnom praní môže zafixovať",
        "main_textile": "bielych tričkách, obliečkach, uterákoch, košeliach a svetlej bielizni zo sušiaka",
        "avoid": "chlórové bielidlo, žehlenie a opakované sušenie na rovnakom hrdzavom mieste",
        "diagnosis": [
            "<strong>Hľadajte zdroj:</strong> štipec, kovová šnúra, sušiak alebo zábradlie môže fľak prenášať opakovane.",
            "<strong>Hrdza sa správa inak:</strong> nestačí ju prať ako blato alebo prach.",
            "<strong>Biela bielizeň klame:</strong> agresívne bielenie môže problém zhoršiť.",
            "<strong>Najprv odstráňte príčinu:</strong> inak sa fľaky vrátia pri ďalšom sušení.",
        ],
        "state_rows": [
            ("malý bod od štipca", "riešiť lokálne a skontrolovať štipec", "zdroj vyhoďte"),
            ("čiara od šnúry", "neprať naslepo s celou dávkou", "skontrolovať kovové časti"),
            ("starší hrdzavý tieň", "postupovať mierne opakovane", "teplo nepomáha"),
            ("biele obliečky", "bez chlóru a bez žehlenia pred kontrolou", "riziko zafixovania"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "lokálne ošetriť a prať podľa štítku", "odolnejšie, ale fľak vidno"),
            ("obliečky", "skontrolovať miesto po šnúre", "veľká plocha sa ľahko prehliadne"),
            ("uterák", "pozor na slučky a okraje", "hrdza sa drží v štruktúre"),
            ("košeľa", "netrieť ostrou kefou", "môže vzniknúť svetlý poškodený kruh"),
        ],
        "sections": [
            ("Ako vznikajú hrdzavé fľaky od štipcov", "Najčastejšie ide o starý kovový mechanizmus štipca, poškodenú šnúru, sušiak alebo kontakt s kovovým zábradlím. Vlhký textil vytiahne kovové zafarbenie a po vyschnutí ostane oranžový alebo hnedý bod.", "Ak zdroj nevyradíte, rovnaký problém sa bude vracať. Preto má zmysel skontrolovať štipce skôr, než začnete riešiť samotné pranie."),
            ("Ako odstrániť hrdzu z bielej bielizne", "Pri bielej bielizni je lákavé siahnuť po silnom bielení, ale pri hrdzi to nemusí byť správny prvý krok. Hrdza potrebuje lokálne ošetrenie a trpezlivú kontrolu, nie automaticky agresívny zásah.", "Pred žehlením alebo sušičkou musí byť fľak preč alebo aspoň výrazne oslabený. Teplo môže zostávajúci tieň zafixovať."),
            ("Hrdzavá čiara od šnúry na bielizeň", "Ak je na textile dlhšia čiara, zdrojom môže byť šnúra, kovové jadro, rám sušiaka alebo kontakt so zábradlím. Vtedy nečistite iba jeden kus oblečenia, ale aj miesto, kde sušíte.", "K údržbe sušiaka nadväzuje článok <a href=\"/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo\">ako vyčistiť sušiak na bielizeň</a>."),
            ("Čo nerobiť pri hrdzavom fľaku", "Nedrhnite fľak silno do strán a nesnažte sa ho prekryť parfumáciou alebo avivážou. Hrdza je farebný minerálny problém, nie pach. Nesprávny postup môže zanechať svetlý kruh alebo poškodený povrch látky.", "Ak ide o drahý kus alebo jemnú látku, radšej postupujte po malých krokoch a testujte na skrytom mieste."),
            ("Prevencia pri sušení vonku", "Štipce pravidelne obmieňajte, kovové časti sušiaka kontrolujte a svetlé textílie nevešajte na miesto, ktoré už raz zanechalo stopu. Pri vonkajšom sušení sledujte aj zábradlie, parapet a kovové držiaky.", "Najlacnejšia oprava hrdzavých škvŕn je vyhodiť problémové štipce skôr, než zničia ďalšiu bielizeň."),
        ],
        "depth": [
            ("Prečo hrdza nie je obyčajná škvrna", "Hrdza je zafarbenie z kovu, ktoré sa správa inak než mastnota, blato alebo jedlo. Preto bežné pranie môže odstrániť povrchovú špinu, ale oranžový tón nechá v látke.", "Dôležité je nepomýliť si hrdzu s prachom. Ak sa škvrna objavuje presne v mieste štipca alebo šnúry, riešte zdroj."),
            ("Kontrola po praní a pred žehlením", "Hrdzavé fľaky kontrolujte pri dennom svetle. Mokrá bielizeň môže vyzerať lepšie, ale po vyschnutí sa oranžový tieň vráti. Žehličku použite až vtedy, keď je výsledok skutočne čistý.", "Pri opakovanom výskyte si označte problémové miesto na sušiaku a nepoužívajte ho na svetlé kusy."),
        ],
        "expert_title": "Odbornejší pohľad: kovové zafarbenie, voda a kontakt pri sušení",
        "expert_p1": "Pri hrdzi je podstatný kontakt vlhkého textilu s kovovým zdrojom. Voda pomáha preniesť zafarbenie na vlákno a po vysušení ostane stopa. Preto je okrem čistenia textilu dôležité odstrániť aj zdroj kontaminácie.",
        "rule": "Pri hrdzi najprv nájdite štipec, šnúru alebo sušiak, ktorý fľak spôsobil. Bez odstránenia zdroja budete čistiť stále tie isté škvrny.",
        "recommendation_intro": "Pri hrdzavých fľakoch má prací gél zmysel ako následné pranie po lokálnom ošetrení a po odstránení zdroja hrdze. Samotný prací cyklus bez predprípravy nemusí stačiť.",
        "product_text": "Vhodný na následné pranie bielizne, obliečok a uterákov po lokálnom ošetrení hrdzavého fľaku. Pri citlivých materiáloch vždy rešpektujte štítok.",
        "links": [
            ("/n/ako-odstranit-hrdzu-z-oblecenia-obrusu-a-pracovnych-nohavic", "Ako odstrániť hrdzu z oblečenia, obrusu a pracovných nohavíc"),
            ("/n/ako-vycistit-susiak-na-bielizen-aby-neprenasal-spinu-na-pradlo", "Ako vyčistiť sušiak na bielizeň, aby neprenášal špinu na prádlo"),
            ("/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou", "Ako správne prať obliečky"),
        ],
        "faq": [
            ("Prečo sa hrdzavý fľak objavil až po sušení?", "Vlhká látka sa dotkla hrdzavého štipca, šnúry alebo kovovej časti sušiaka a zafarbenie sa prenieslo počas schnutia."),
            ("Môžem použiť chlórové bielidlo?", "Pri hrdzi to nie je bezpečný univerzálny prvý krok. Najprv riešte lokálne a podľa materiálu."),
            ("Ako zabrániť návratu fľakov?", "Vymeňte štipce, skontrolujte šnúru a vyčistite alebo vyraďte hrdzavé kovové časti sušiaka."),
        ],
    },
    "nail_polish": {
        "marker": "Detailnejší postup na lak na nechty, rozpúšťadlá a rozmazanie škvrny",
        "problem": "lak na nechty na textile rýchlo tuhne, lepí sa na povrch a pri nesprávnom zásahu sa rozmaže do väčšej farebnej mapy",
        "main_textile": "tričku, legínach, uteráku, obliečke, sukni a jemnej blúzke",
        "avoid": "okamžité trenie do strán, horúcu vodu a neotestované rozpúšťadlo na farebnom alebo jemnom textile",
        "diagnosis": [
            "<strong>Najprv zastaviť rozmazanie:</strong> lak sa nesmie vtlačiť do väčšej plochy.",
            "<strong>Materiál rozhoduje:</strong> acetát, jemné zmesi a farbené látky môžu reagovať citlivo.",
            "<strong>Podložte škvrnu:</strong> lak sa môže preniesť na druhú stranu látky.",
            "<strong>Sušička až po kontrole:</strong> zvyšok laku sa teplom spevní.",
        ],
        "state_rows": [
            ("čerstvá kvapka", "odobrať prebytok bez rozotretia", "pracovať zhora"),
            ("zaschnutý lak", "opatrne odlúpnuť iba voľný okraj", "netrhať vlákna"),
            ("farebný tieň", "riešiť po teste materiálu", "pozor na púšťanie farby"),
            ("jemná látka", "zvážiť čistiareň", "riziko poškodenia povrchu"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "podložiť a pracovať od okraja", "odolnejšie, ale drží pigment"),
            ("uterák", "nevtierať do slučiek", "lak sa zachytí v štruktúre"),
            ("syntetika", "testovať postup", "niektoré vlákna reagujú citlivo"),
            ("jemná blúzka", "bez agresívneho trenia", "riziko mapy alebo lesku"),
        ],
        "sections": [
            ("Ako odstrániť lak na nechty z trička", "Tričko podložte savou handričkou a prebytok laku riešte z povrchu, nie trením do strán. Ak je lak mokrý, každý prudký pohyb zväčší škvrnu. Ak je zaschnutý, odstraňujte iba to, čo ide dole bez trhania vlákien.", "Pred použitím akéhokoľvek rozpúšťadla urobte test na skrytom mieste. Farba trička alebo potlač môže reagovať inak než samotná škvrna."),
            ("Lak na nechty na uteráku alebo obliečke", "Froté uterák a obliečka sa správajú rozdielne. Uterák má slučky, do ktorých sa lak zachytí, obliečka má väčšiu hladkú plochu, na ktorej sa škvrna môže rozmazať. V oboch prípadoch pomáha podložiť miesto a pracovať postupne.", "Ak na bielej obliečke zostane farebný tieň, nesušte ju horúco. Najprv zopakujte lokálnu kontrolu."),
            ("Prečo nie je každý odlakovač bezpečný", "Odlakovač je určený na nechty, nie automaticky na textil. Niektoré materiály, farbivá alebo potlače môžu zmeniť farbu, lesk alebo štruktúru. Preto je test na skrytom mieste povinný, nie formálny detail.", "Pri jemných a drahších látkach je bezpečnejšie zastaviť domáci zásah skôr než poškodiť povrch."),
            ("Ako zabrániť rozmazaniu škvrny", "Pracujte od okraja ku stredu a často meňte savú podložku. Ak podklad nasiakne farbou, môže sa škvrna preniesť späť na textil. Menej tlaku a viac trpezlivosti býva lepšie než jeden silný zásah.", "Podobný princíp platí pri iných farebných škvrnách, napríklad pri <a href=\"/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania\">akrylovej farbe</a>."),
            ("Kedy lak na nechty neriešiť doma", "Ak je škvrna na hodvábe, viskózovej blúzke, saku, vlne alebo textile s nestálou farbou, domáce rozpúšťadlá sú rizikové. Vtedy je lepšie škvrnu nerozširovať a obrátiť sa na profesionálne čistenie.", "Najhorší scenár je väčšia svetlá mapa okolo pôvodnej malej škvrny. Preto vždy najprv testujte."),
        ],
        "depth": [
            ("Lak ako pevný film na povrchu vlákna", "Lak na nechty po zaschnutí vytvorí pevnejšiu vrstvu. Tá môže sedieť na povrchu látky alebo sa dostať hlbšie do väzby. Pri praní bez predprípravy sa nemusí rozpustiť, iba sa spevní alebo rozláme.", "Preto je dôležité pracovať s malou plochou, podložkou a kontrolou materiálu."),
            ("Ako kontrolovať výsledok po praní", "Po praní sledujte nielen farbu, ale aj tvrdší povrch. Ak miesto zostalo mierne lesklé alebo drsné, lak nemusí byť úplne preč. Sušičku a žehličku odložte, kým si výsledok neoveríte.", "Pri opakovanom pokuse znižujte mechanické trenie. Poškodené vlákno už nevyčistíte späť do pôvodného stavu."),
        ],
        "expert_title": "Odbornejší pohľad: rozpúšťanie laku a citlivosť textilných vlákien",
        "expert_p1": "Lak na nechty je filmotvorný produkt. To znamená, že po zaschnutí drží ako súvislá vrstva a pri kontakte s niektorými látkami alebo farbami môže zanechať stopu aj po odstránení objemu. Preto je test materiálu dôležitejší než univerzálny trik.",
        "rule": "Pri laku na nechty je cieľom zabrániť rozmazaniu. Najprv test, potom malá plocha, savá podložka a až na konci pranie podľa štítku.",
        "recommendation_intro": "Po bezpečnom lokálnom odstránení laku má prací gél pomôcť vyprať zvyšky z textilu. Nenahrádza test materiálu ani opatrné predčistenie.",
        "product_text": "Vhodný na následné pranie bežných textílií po lokálnom ošetrení laku na nechty. Pri syntetike, viskóze a jemných látkach najprv rozhoduje štítok a test.",
        "links": [
            ("/n/ako-odstranit-ruz-z-kosele-salu-a-latkovej-servitky", "Ako odstrániť rúž z košele, šálu a látkovej servítky"),
            ("/n/ako-odstranit-akrylovu-farbu-z-tricka-bez-zafixovania", "Ako odstrániť akrylovú farbu z trička bez zafixovania"),
            ("/n/ako-vyprat-voskovky-z-peracnika-a-textilneho-obalu", "Ako vyprať voskovky z peračníka a textilného obalu"),
        ],
        "faq": [
            ("Mám lak na nechty hneď šúchať?", "Nie. Najprv zabráňte rozmazaniu a podložte škvrnu. Trením do strán ju zväčšíte."),
            ("Je odlakovač vždy bezpečný?", "Nie. Na farebných, jemných a syntetických látkach ho najprv testujte na skrytom mieste."),
            ("Prečo ostal tvrdý bod aj po praní?", "Na vlákne zostal zvyšok filmu. Miesto nesušte horúco a postup opatrne zopakujte."),
        ],
    },
    "travel_clothes": {
        "marker": "Detailnejší postup na cestovné oblečenie po lietadle, vlaku a dlhom sedení",
        "problem": "cestovné oblečenie po dlhom lete alebo vlaku kombinuje pot, prach, mastnotu z pokožky, pach batožiny a stlačenie materiálu",
        "main_textile": "tričkách, mikinách, legínach, nohaviciach, šatkách a cestovných vrstvách",
        "avoid": "nechať vlhké a spotené veci zatvorené v kufri alebo ich prevoňať bez vyprania",
        "diagnosis": [
            "<strong>Najprv vyvetrať:</strong> oblečenie po ceste nedávajte hneď do uzavretého koša.",
            "<strong>Rozlíšte pach a škvrnu:</strong> podpazušie, golier a pás potrebujú kontrolu.",
            "<strong>Prať podľa materiálu:</strong> merino, viskóza, polyester a bavlna neznášajú rovnaký režim.",
            "<strong>Kufor je súčasť problému:</strong> pach sa môže prenášať aj z batožiny.",
        ],
        "state_rows": [
            ("iba zatuchnutie", "vyvetrať a prať v menšej dávke", "neprevoňať naslepo"),
            ("pot v podpazuší", "lokálne predčistiť", "kontrola pred sušením"),
            ("prach a sedadlo", "vytriasť a prať naruby", "najmä tmavé veci"),
            ("jemná cestovná vrstva", "riadiť sa štítkom", "pozor na krčenie"),
        ],
        "textile_rows": [
            ("bavlnené tričko", "predčistiť podpazušie a golier", "pot a maz sa držia v úplete"),
            ("legíny", "prať naruby bez preplnenia", "syntetika drží pach"),
            ("mikina", "skontrolovať kapucňu a manžety", "dotyk s batožinou"),
            ("šatka", "šetrný program a vzdušné sušenie", "drží parfum aj pach cesty"),
        ],
        "sections": [
            ("Ako triediť oblečenie po návrate z cesty", "Po príchode domov nedávajte všetko z kufra rovno do jednej dávky. Oddelte spodné vrstvy, veci so spoteným podpazuším, jemné šatky a oblečenie, ktoré bolo iba čisté, ale zatvorené v batožine. Každá skupina potrebuje trochu iný prístup.", "Najviac pozornosti venujte tričkám, legínam, golierom a pásom nohavíc. Tam sa spája pot, maz a trenie zo sedenia."),
            ("Ako prať oblečenie po dlhom lete", "Po lete býva problémom dlhé sedenie, suchý vzduch, pot a pach batožiny. Oblečenie najprv vyvetrajte, potom skontrolujte miesta pri tele a perte v primerane plnom bubne. Preplnená práčka zhorší oplach aj odstránenie pachu.", "Ak ostáva pach v syntetike, pomôže nadviazať na návod <a href=\"/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu\">ako odstrániť zápach z bežeckých legín</a>."),
            ("Ako prať oblečenie po vlaku alebo autobuse", "Vo vlaku a autobuse sa oblečenie viac dotýka sedadiel, batožiny a vrchných vrstiev. Pred praním veci vytraste, otočte naruby a skontrolujte lokálne škvrny. Prach sa pri mokrom praní môže zachytiť na svetlých kusoch.", "Pri nohaviciach a mikinách sledujte najmä manžety, spodné lemy a miesta, ktoré sa dotýkali batožiny."),
            ("Cestovné šatky, mikiny a vrstvenie", "Šatka alebo mikina môže byť na ceste použitá ako prikrývka, vankúš alebo vrstva proti klimatizácii. Preto často drží pach viac než tričko. Perte ju podľa materiálu a sušte vzdušne, aby nezostala zatuchnutá.", "Súvisiaca téma je <a href=\"/n/ako-odstranit-zapach-z-cestovneho-vankusa-po-lietadle\">ako odstrániť zápach z cestovného vankúša</a>."),
            ("Kufor, sáčky a opätovné zatuchnutie", "Ak čisté oblečenie po praní vrátite do zatuchnutého kufra, problém sa môže vrátiť. Po návrate nechajte kufor otvorený, vyberte textilné organizéry a skontrolujte, či v batožine nezostala vlhká vec.", "Pri častom cestovaní sa oplatí oddeliť použité oblečenie do priedušného vrecka, nie do igelitky, kde sa drží vlhkosť."),
        ],
        "depth": [
            ("Prečo cestovné oblečenie zapácha inak než bežná bielizeň", "Pri cestovaní sa pot a maz mieša s dlhým sedením, prachom, klimatizáciou a uzavretou batožinou. Textil potom nezapácha iba po tele, ale aj po priestore, v ktorom bol zatvorený.", "Preto nestačí pridať viac vône. Najprv treba zabezpečiť dobrý oplach, primeranú dávku prania a rýchle sušenie."),
            ("Ako kontrolovať výsledok po praní", "Po praní skontrolujte podpazušie, golier a pás nohavíc ešte pred sušičkou. Ak je textil stále cítiť, nechajte ho preschnúť vzdušne a zvážte opakovanie šetrného prania, nie vyššiu dávku vône.", "Pri syntetických materiáloch je lepšie prať menšiu dávku a dať priestoru na oplach než preplniť bubon cestovnými vecami naraz."),
        ],
        "expert_title": "Odbornejší pohľad: pach, vlhkosť a uzavretá batožina",
        "expert_p1": "Zápach po cestovaní vzniká kombináciou biologických zvyškov, prachu a zadržiavanej vlhkosti v uzavretom priestore. Keď oblečenie zostane dlho v kufri, pach sa stabilizuje a obyčajné krátke prevoňanie nemusí stačiť.",
        "rule": "Pri cestovnom oblečení najprv vyvetrajte a roztrieďte veci podľa kontaktu s telom. Potom perte menšie dávky tak, aby sa textil skutočne opláchol.",
        "recommendation_intro": "Pri oblečení po cestovaní je dôležitý dobrý oplach a primerané dávkovanie. Prací gél pomôže až vtedy, keď veci neostanú zatvorené vlhké v kufri alebo koši.",
        "product_text": "Vhodný na následné pranie tričiek, mikín, legín a cestovných vrstiev po vyvetraní a roztriedení. Pri funkčných a jemných materiáloch rešpektujte štítok.",
        "links": [
            ("/n/ako-odstranit-zapach-z-cestovneho-vankusa-po-lietadle", "Ako odstrániť zápach z cestovného vankúša po lietadle"),
            ("/n/ako-odstranit-zapach-z-bezeckych-legin-po-treningu", "Ako odstrániť zápach z bežeckých legín po tréningu"),
            ("/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia", "Prečo moje oblečenie zapácha po praní"),
        ],
        "faq": [
            ("Treba cestovné oblečenie prať hneď po návrate?", "Spotenené spodné vrstvy áno. Čisté, ale zatvorené veci najprv vyvetrajte a potom rozhodnite podľa pachu a materiálu."),
            ("Prečo oblečenie po lete cítiť aj po praní?", "Mohlo byť preplnené v bubne, zle opláchnuté alebo zostalo dlho vlhké v kufri."),
            ("Ako zabrániť zatuchnutiu v batožine?", "Použité veci oddeľte do priedušného vrecka, kufor po návrate vyvetrajte a vlhké kusy nenechávajte zatvorené."),
        ],
    },
}


def build_expansion(topic):
    config = TOPICS[topic]
    sections = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["sections"])
    depth = "\n".join(f"<h2>{title}</h2>\n<p>{first}</p>\n<p>{second}</p>" for title, first, second in config["depth"])
    faq = "\n".join(f"<h3>{question}</h3>\n<p>{answer}</p>" for question, answer in config["faq"])
    return clean(
        f"""
        <h2>{config["marker"]}</h2>
        <p>{config["problem"].capitalize()}. Preto sa neoplatí začínať iba hlavným pracím cyklom. Najprv rozlíšte, či riešite mastnotu, pigment, film, kovové zafarbenie, pach alebo kombináciu viacerých vrstiev.</p>
        <p>Pri textile ako {config["main_textile"]} rozhoduje aj materiál, farba, väzba a to, čo sa s vecou stane po praní. Najväčšie riziko je {config["avoid"]}.</p>
        {note_card("Rýchla praktická diagnostika", config["diagnosis"])}
        <h2>Postup podľa stavu škvrny alebo problému</h2>
        {table(["Stav", "Čo urobiť", "Poznámka"], config["state_rows"])}
        <h2>Postup podľa typu textilu</h2>
        {table(["Textil", "Postup", "Prečo"], config["textile_rows"])}
        {sections}
        <h2>{config["expert_title"]}</h2>
        <p>{config["expert_p1"]}</p>
        <p>Praktické databázy škvŕn odporúčajú posudzovať typ škvrny, typ textilu a kontrolu pred sušením. Užitočný odborný zdroj k princípom domáceho predčistenia je <a rel="noopener" href="https://extension.illinois.edu/global/stain-solutions" target="_blank">Illinois Extension Stain Solutions</a>.</p>
        {depth}
        <h2>Kontrolný checklist pred praním</h2>
        <p>Pred praním skontrolujte, či na látke nezostal objem škvrny, mastný alebo lepkavý pocit, farebný okraj, tuhý film alebo neobvyklý pach. Pozrite si aj rubovú stranu, lemy, švy, golier, manžety a miesta, ktoré sa dotýkali pokožky alebo vonkajšieho zdroja znečistenia.</p>
        <p>Do jednej dávky nedávajte kusy s nevyriešenou mastnotou, pigmentom, hrdzou a pachom. Každý problém sa správa inak a môže ovplyvniť ostatné oblečenie. Ak najprv vyriešite najťažšie miesto, samotné pranie potom dokončí hygienu, oplach a sviežosť spoľahlivejšie.</p>
        <h2>Kedy postup zopakovať</h2>
        <p>Ak po praní zostal tieň, klzký pocit, tvrdší povrch, oranžový bod alebo pach, nesušte textil horúco. Zopakujte lokálne predčistenie a perte podľa štítku. Opakovaný mierny postup býva bezpečnejší než jeden agresívny zásah.</p>
        <p>Ak látka púšťa farbu, mení povrch, leskne sa alebo ide o drahší kus, zastavte domáce experimentovanie skôr. Cieľom je zachovať textil použiteľný, nie odstrániť škvrnu za cenu poškodenia materiálu.</p>
        <h2>Ako predísť zafixovaniu pri sušení</h2>
        <p>Mokrá látka môže vyzerať čistejšie, než v skutočnosti je. Mastnota, pigment, film alebo hrdza sa často ukážu až po preschnutí. Preto kontrolujte miesto pri dennom svetle a sušičku použite až vtedy, keď je výsledok čistý.</p>
        <p>Ak máte pochybnosť, nechajte kus vyschnúť voľne bez tepla a potom sa rozhodnite, či treba ďalšie lokálne predčistenie. Tento postup je pomalší, ale znižuje riziko trvalej mapy.</p>
        <h2>Domáca rutina pri opakovaných problémoch</h2>
        <p>Ak sa podobný problém opakuje, vytvorte si jednoduchý postup: rýchla kontrola pred košom na bielizeň, odstránenie povrchových zvyškov, lokálne predčistenie, pranie v primerane plnom bubne a kontrola pred sušením. Pri obrusoch, kozmetike, hrdzi aj cestovaní je poradie krokov často rozhodujúce.</p>
        <p>Všímajte si aj miesto vzniku: golier pri vlasoch, obrus po omáčke, štipec na sušiaku alebo kufor po ceste. Prevencia potom nie je všeobecná rada, ale konkrétny zvyk pred praním.</p>
        <h2>Čo sledovať po druhom praní</h2>
        <p>Ak sa problém po druhom šetrnom praní stále vracia, sledujte, či ide o farbu, mastnotu, pach alebo zmenu povrchu látky. Farebný tieň potrebuje iný prístup než klzký film a poškodený povrch už nie je škvrna, ale zmena materiálu.</p>
        <p>Pri opakovaných škvrnách si zapisujte, čo fľak spôsobilo a ktorý krok pomohol. Pri ďalšom praní tak nebudete začínať od nuly a znížite riziko zbytočne agresívneho zásahu.</p>
        <div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
        <h2 style="margin-top: 0;">Rýchla zásada</h2>
        <p>{config["rule"]}</p>
        </div>
        {recommendation_card(config)}
        {build_related_links(config["links"])}
        <h2>FAQ: praktické otázky</h2>
        {faq}
        """
    )


MARKERS = {key: value["marker"] for key, value in TOPICS.items()}
EXPANSIONS = {key: build_expansion(key) for key in TOPICS}


def article_slug(article):
    if article.get("link"):
        return article["link"]
    if article.get("slug"):
        return article["slug"]
    if article.get("url"):
        return article["url"].rstrip("/").split("/")[-1]
    return ""


def load_source(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data, data
    if isinstance(data, dict) and isinstance(data.get("updates"), list):
        return data, data["updates"]
    raise SystemExit(f"Unsupported source format: {path}")


PUBLIC_REPLACEMENTS = [
    (
        re.compile(r"<p>\s*Pokryté výrazy:\s*(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré sa pri tejto škvrne často riešia: \1.</p>",
    ),
    (
        re.compile(r"<p>\s*Článok cieli výrazy ako\s+(.*?)\.\s*</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické situácie, ktoré ľudia pri tejto téme často riešia: \1.</p>",
    ),
    (
        re.compile(r"<p>\s*V článku pokrývame aj praktické otázky z praxe:\s*<strong>(.*?)</strong>\.\s*(.*?)</p>", re.IGNORECASE | re.DOTALL),
        r"<p>V texte nájdete aj praktické otázky z praxe: <strong>\1</strong>. \2</p>",
    ),
]


def public_cleanup(long):
    cleaned = long
    for pattern, replacement in PUBLIC_REPLACEMENTS:
        cleaned = pattern.sub(replacement, cleaned)
    return cleaned


def insertion_index(long):
    candidates = [
        long.find('<div style="border: 1px solid #dbe5de'),
        long.find("\n<h2>Súvisiace"),
        long.find("\n<h2>FAQ"),
    ]
    candidates = [index for index in candidates if index != -1]
    if not candidates:
        raise ValueError("Could not find safe insertion point")
    return min(candidates)


def insert_expansion(long, key):
    long = public_cleanup(long)
    if MARKERS[key] in long:
        start = long.find(f"<h2>{MARKERS[key]}</h2>")
        faq_start = long.find("<h2>FAQ: praktick", start)
        search_from = faq_start if faq_start != -1 else start + len(MARKERS[key])
        candidates = [
            long.find('<div style="border: 1px solid #dbe5de', search_from),
            long.find("\n<h2>Súvisiace", search_from),
            long.find("\n<h2>FAQ", search_from + 1),
        ]
        candidates = [index for index in candidates if index != -1]
        if not candidates:
            raise ValueError("Could not find safe replacement end point")
        end = min(candidates)
        return long[:start].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[end:].lstrip()
    index = insertion_index(long)
    return long[:index].rstrip() + "\n" + EXPANSIONS[key] + "\n" + long[index:].lstrip()


def mcp_url():
    config = CONFIG_PATH.read_text(encoding="utf-8")
    match = re.search(r'(?s)\[mcp_servers\.biznisweb-vevo\]\s*url\s*=\s*"([^"]+)"', config)
    if not match:
        raise SystemExit("biznisweb-vevo MCP URL not found in ~/.codex/config.toml")
    return match.group(1)


def parse_sse_json(text):
    for line in text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    raise ValueError(f"No JSON data line in MCP response: {text[:500]}")


def call_update(endpoint, payload, request_id):
    body = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "tools/call",
        "params": {"name": "biznisweb-update_news_post", "arguments": payload},
    }
    response = requests.post(
        endpoint,
        json=body,
        headers={"Accept": "application/json, text/event-stream"},
        timeout=120,
    )
    response.raise_for_status()
    parsed = parse_sse_json(response.text)
    if "error" in parsed:
        raise RuntimeError(json.dumps(parsed["error"], ensure_ascii=False))
    result = parsed.get("result", {})
    for item in result.get("content", []):
        if item.get("type") != "text":
            continue
        try:
            inner = json.loads(item.get("text", ""))
        except json.JSONDecodeError:
            continue
        if inner.get("error"):
            raise RuntimeError(inner["error"])
    return parsed


def main():
    parser = argparse.ArgumentParser(description="Conservatively expand VEVO retrofit wave 17 mixed stain and travel articles.")
    parser.add_argument("--update-live", action="store_true")
    parser.add_argument("--sleep", type=float, default=1.5)
    args = parser.parse_args()

    loaded = {}
    updates = []

    for config in ARTICLES:
        source = config["source"]
        if source not in loaded:
            loaded[source] = load_source(source)
        data, rows = loaded[source]

        for article in rows:
            if article_slug(article) != config["slug"]:
                continue
            original = {
                "title": article.get("title"),
                "short": article.get("short", ""),
                "slug": article_slug(article),
                "date_posted": article.get("date_posted"),
                "time_posted": article.get("time_posted"),
                "active": article.get("active"),
                "link": article.get("link"),
                "url": article.get("url"),
            }
            original_long = article["long"]
            article["long"] = insert_expansion(article["long"], config["topic"])
            if (
                article.get("title") != original["title"]
                or article_slug(article) != original["slug"]
                or article.get("short", "") != original["short"]
                or article.get("date_posted") != original["date_posted"]
                or article.get("time_posted") != original["time_posted"]
                or article.get("active") != original["active"]
                or article.get("link") != original["link"]
            ):
                raise SystemExit(f"Retrofit attempted to change protected metadata for {config['slug']}")
            if original["url"] and article.get("url") != original["url"]:
                raise SystemExit(f"Retrofit attempted to change URL for {config['slug']}")
            updates.append(
                {
                    "post_id": config["post_id"],
                    "slug": config["slug"],
                    "url": config["url"],
                    "title": article["title"],
                    "short": article["short"],
                    "long": article["long"],
                    "source_file": str(source.relative_to(ROOT)),
                    "original_length": len(original_long),
                    "new_length": len(article["long"]),
                    "title_preserved": True,
                    "slug_preserved": True,
                    "url_preserved": True,
                    "short_preserved": True,
                    "date_preserved": True,
                    "visibility_preserved": True,
                }
            )
            break
        else:
            raise SystemExit(f"Article not found: {config['slug']}")

    for source, (data, _) in loaded.items():
        source.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    OUT_JSON.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-17-mixed-stains-travel-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "purpose": "Conservative additive expansion. Titles, slugs, URLs, dates, visibility, and short descriptions are preserved.",
                "updates": updates,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    mcp_updates = []
    if args.update_live:
        endpoint = mcp_url()
        for index, item in enumerate(updates, start=1):
            result = call_update(
                endpoint,
                {
                    "post_id": item["post_id"],
                    "title": item["title"],
                    "short": item["short"],
                    "long": item["long"],
                    "visible": True,
                },
                index,
            )
            mcp_updates.append({"post_id": item["post_id"], "slug": item["slug"], "url": item["url"], "mcp_result": result.get("result", result)})
            time.sleep(args.sleep)

    MCP_RESULTS.write_text(
        json.dumps(
            {
                "project": "VEVO_CONTENT",
                "wave": "retrofit-wave-17-mixed-stains-travel-five",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "live_updated": args.update_live,
                "updated_count": len(mcp_updates),
                "updates": mcp_updates,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "source_updates": len(updates),
                "live_updated": args.update_live,
                "mcp_updates": len(mcp_updates),
                "out": str(OUT_JSON),
                "mcp_results": str(MCP_RESULTS),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

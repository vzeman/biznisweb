import html
import json
import re
from pathlib import Path
from urllib.parse import urljoin

import requests


BASE_URL = "https://www.vevo.sk"
PUBLISH_DATE = "2026-07-14"
CANDIDATES_FILE = Path(
    "content/VEVO_CONTENT/batches/batch-39-test-candidate-2026-07-14.txt"
)
ARTICLES_FILE = Path(
    "content/VEVO_CONTENT/imports/batch-39-test-gsm-2026-07-14-articles.json"
)
PREFLIGHT_FILE = Path(
    "content/VEVO_CONTENT/exports/batch-39-test-gsm-2026-07-14-link-preflight.json"
)

TITLE = "Gramáž látky: čo znamená GSM pri uterákoch, obliečkach a tričkách"
SLUG = "gramaz-latky-co-znamena-gsm-pri-uterakoch-oblieckach-a-trickach"
META_DESCRIPTION = (
    "Čo znamená GSM a gramáž látky pri uterákoch, obliečkach a tričkách: "
    "výpočet, praktické rozsahy, výber, pranie aj limity porovnávania."
)
SHORT_DESCRIPTION = (
    "GSM vyjadruje hmotnosť jedného štvorcového metra textílie, nie automaticky "
    "jej kvalitu. V článku zistíte, ako gramáž čítať pri uterákoch, posteľnej "
    "bielizni a tričkách, prečo ju treba hodnotiť spolu s vláknom a väzbou a čo "
    "mení pri praní, sušení a každodennom používaní."
)

FORBIDDEN_PUBLIC_RE = re.compile(
    r"longtail|long-tail|long tail|\bkeyword(?:s)?\b|kľúčov\w*\s+slov\w*|"
    r"\bSEO\b|search\s+intent|sub[- ]?quer(?:y|ies)|sub[- ]?query|"
    r"fan[- ]?out|fanout|\bCTA\b",
    re.IGNORECASE,
)


def table(headers, rows):
    head = "".join(
        '<th style="border: 1px solid #e5e5e5; padding: 10px; '
        f'text-align: left;">{html.escape(header)}</th>'
        for header in headers
    )
    body = "\n".join(
        "<tr>"
        + "".join(
            '<td style="border: 1px solid #e5e5e5; padding: 10px; '
            f'vertical-align: top;">{cell}</td>'
            for cell in row
        )
        + "</tr>"
        for row in rows
    )
    return (
        '<table style="width: 100%; border-collapse: collapse; margin: 20px 0;">\n'
        f"<thead><tr>{head}</tr></thead>\n<tbody>\n{body}\n</tbody>\n</table>"
    )


def render_article():
    gsm_ranges = table(
        ["Textília", "Orientačný rozsah", "Čo si pri výbere všímať"],
        [
            (
                "Ľahšie uteráky",
                "približne 300 až 450 GSM",
                "Rýchlejšie schnutie a menší objem; výslednú savosť výrazne mení "
                "materiál, výška slučky a spracovanie.",
            ),
            (
                "Univerzálne kúpeľňové uteráky",
                "približne 450 až 600 GSM",
                "Vyváženie savosti, objemu a času schnutia pre bežnú domácnosť.",
            ),
            (
                "Veľmi hutné uteráky a osušky",
                "približne 600 GSM a viac",
                "Mäkký, plný pocit, ale väčšia mokrá hmotnosť a dlhšie sušenie.",
            ),
            (
                "Ľahšie tričká",
                "približne 120 až 160 GSM",
                "Vzdušnosť, splývavosť a možná priehľadnosť závisia aj od farby "
                "a pleteniny.",
            ),
            (
                "Stredne ťažké tričká",
                "približne 160 až 200 GSM",
                "Univerzálna voľba; porovnávajte zloženie, strih a hustotu očka.",
            ),
            (
                "Ťažšie tričká",
                "približne 200 GSM a viac",
                "Pevnejší, štruktúrovanejší pocit, viac materiálu a pomalšie schnutie.",
            ),
        ],
    )

    decision_table = table(
        ["Situácia", "Praktická voľba", "Dôvod a kontrolná otázka"],
        [
            (
                "Malá kúpeľňa bez dobrého vetrania",
                "skôr ľahší až stredný uterák",
                "Vyschne medzi použitiami? Máte miesto, kde môže visieť rozprestretý?",
            ),
            (
                "Uterák pre hostí alebo wellness pocit",
                "stredná až vyššia gramáž",
                "Je slučka pružná a uterák sa po stlačení vráti, alebo je iba "
                "chemicky zmäkčený?",
            ),
            (
                "Obliečky na horúce leto",
                "ľahšia priedušná tkanina",
                "Gramáž je iba časť odpovede; rozhoduje vlákno, väzba a schopnosť "
                "odvádzať vlhkosť.",
            ),
            (
                "Obliečky do chladnejšej spálne",
                "hutnejšia alebo česaná konštrukcia",
                "Flanel môže hriať vďaka povrchu, hoci samotné číslo GSM nevysvetlí "
                "celý tepelný pocit.",
            ),
            (
                "Tričko na leto a vrstvenie",
                "ľahšia až stredná gramáž",
                "Je úplet dostatočne nepriehľadný a drží tvar po natiahnutí?",
            ),
            (
                "Tričko s pevnou siluetou",
                "stredná až vyššia gramáž",
                "Vyhovuje vám vyššia hmotnosť, menšia splývavosť a dlhšie schnutie?",
            ),
        ],
    )

    return f"""
<p><strong>Rýchla odpoveď:</strong> Skratka <strong>GSM</strong> znamená gramy na meter štvorcový. Textília s hodnotou 500 GSM má teda pri ploche jedného štvorcového metra hmotnosť 500 gramov. Vyššie číslo zvyčajne znamená viac materiálu na rovnakej ploche, nie však automaticky lepšiu kvalitu, väčšiu savosť ani dlhšiu životnosť.</p>
<p>Pri uterákoch pomáha gramáž odhadnúť objem, mokrú hmotnosť a rýchlosť schnutia. Pri tričkách napovie, či bude úplet ľahký a splývavý alebo pevnejší a štruktúrovanejší. Pri obliečkach je užitočná iba spolu s údajom o vlákne, väzbe a povrchovej úprave. Na pranie vždy rozhoduje ošetrovací štítok, nie samotné GSM.</p>

<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Čo z čísla GSM zistíte za pár sekúnd</h2>
<ul>
<li><strong>Hmotnosť na plochu:</strong> porovnáva množstvo materiálu na jednom metri štvorcovom.</li>
<li><strong>Objem a manipuláciu:</strong> vyššia gramáž často znamená objemnejší výrobok, najmä pri podobnej konštrukcii.</li>
<li><strong>Zaťaženie práčky:</strong> hutné uteráky po nasiaknutí výrazne oťažejú a potrebujú priestor na pohyb.</li>
<li><strong>Čas schnutia:</strong> viac materiálu zvyčajne zadrží viac vody, no výsledok mení vlákno, väzba, odstreďovanie a prúdenie vzduchu.</li>
<li><strong>Nie kvalitu:</strong> GSM neodhaľuje pevnosť priadze, kvalitu bavlny, krútenie slučiek, farbostálosť ani kvalitu šitia.</li>
</ul>
</div>

<h2>Čo je gramáž látky a ako sa GSM počíta</h2>
<p>Gramáž textílie je plošná hmotnosť. Namiesto otázky „koľko váži celý uterák?“ sa pýtame „koľko by vážil jeden štvorcový meter tejto textílie?“. Vďaka tomu sa dajú porovnať materiály rôznych rozmerov. Malý uterák môže byť ľahší než veľká osuška, hoci je vyrobený z rovnako hutného froté. Celková hmotnosť výrobku preto nie je to isté ako jeho GSM.</p>
<p>Základný výpočet je jednoduchý: hmotnosť vzorky v gramoch vydelíte jej plochou v metroch štvorcových. Ak má presne vystrihnutá vzorka rozmer 50 × 50 centimetrov, jej plocha je 0,25 m². Pri hmotnosti 125 gramov vychádza 125 ÷ 0,25 = 500 GSM. Pri hotovom výrobku však výsledok skresľujú lemy, zipsy, gombíky, výšivky, etikety aj nerovnomerné vrstvy.</p>

<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 22px 0; background: #f7fbf8;">
<h3 style="margin-top: 0;">Vzorec pre domácu orientáciu</h3>
<p style="font-size: 18px; margin-bottom: 8px;"><strong>GSM = hmotnosť vzorky v gramoch ÷ plocha vzorky v m²</strong></p>
<p style="margin-bottom: 0;">Meranie doma berte ako odhad. Laboratórne metódy pracujú s definovanými vzorkami, kondicionovaním textílie a presnou váhou, aby vlhkosť a spôsob odberu čo najmenej skreslili výsledok.</p>
</div>

<h2>Orientačné gramáže uterákov a tričiek</h2>
<p>Rozsahy v obchodoch nie sú pevné normy kvality. Pomáhajú vytvoriť prvý obraz o tom, ako bude textília pôsobiť, no dva výrobky s rovnakým GSM môžu byť na dotyk aj pri používaní veľmi odlišné. Pri uteráku rozhoduje slučková konštrukcia, druh priadze a úprava. Pri tričku zasa jemnosť vlákna, hustota pleteniny, podiel elastanu, strih a farba.</p>
{gsm_ranges}
<p>Čísla preto používajte na porovnanie výrobkov rovnakej kategórie, nie ako univerzálny rebríček. Uterák s 500 GSM nemožno zmysluplne vyhlásiť za „lepší“ než tričko s 180 GSM. Každý výrobok má inú úlohu a inú konštrukciu.</p>

<h2>Gramáž uteráka: savosť, mäkkosť a rýchlosť schnutia</h2>
<p>Pri froté uteráku vytvárajú povrch slučky, ktoré zväčšujú kontaktnú plochu s vodou. Vyššia gramáž môže znamenať viac alebo dlhšie slučky, prípadne hutnejší základ. Taký uterák často pôsobí plnšie a dokáže prijať viac vody na jeden kus. Samotné číslo však nepovie, ako rýchlo voda prenikne medzi vlákna ani či slučky zostanú pružné po desiatkach praní.</p>
<p>Do malej kúpeľne, na cestovanie alebo do športovej tašky môže byť praktickejší ľahší uterák. Rýchlejšie sa presuší, zaberie menej miesta a práčka zvládne viac kusov bez preťaženia. Veľmi hutná osuška prináša príjemný pocit, ale po použití potrebuje široké zavesenie a dobré vetranie. Ak zostane zložená alebo nahustená na háčiku, jej vysoká schopnosť držať vodu sa môže zmeniť na nevýhodu.</p>
<p>Savosť môže zhoršiť nános zmäkčujúcich látok alebo nadmerné dávkovanie pracieho prostriedku. Pri uterákoch preto sledujte oplach, neprepĺňajte bubon a s avivážou pracujte podľa pokynov výrobcu textílie. Podrobné postupy nájdete v návode <a href="/n/ako-prat-uteraky-rady-a-tipy-na-ciste-a-maekke-uteraky">ako prať uteráky</a> a v článku o tom, <a href="/n/preco-uteraky-zapachaju-aj-po-prani-zatuchnuty-pach-tvrdost-a-strata-savosti">prečo uteráky zapáchajú aj po praní</a>.</p>

<h2>Gramáž posteľnej bielizne: prečo nestačí porovnávať iba jedno číslo</h2>
<p>Pri obliečkach sa spotrebiteľ často stretne skôr s názvom materiálu, typom väzby alebo počtom nití než s GSM. Plošná hmotnosť je stále reálna vlastnosť, ale tepelný a dotykový pocit nevysvetľuje sama. Ľahká husto tkaná bavlna, voľnejšia ľanová tkanina a česaný flanel môžu mať odlišnú priedušnosť, hladkosť aj schopnosť viazať vzduch, hoci sa ich gramáže čiastočne prekrývajú.</p>
<p>Na leto býva príjemná ľahšia, priedušná konštrukcia, ktorá nezadržiava zbytočne veľa tepla a po praní rýchlejšie schne. V zime môže pôsobiť teplejšie hutnejšia alebo česaná textília. Flanel však hreje aj vďaka jemne zdvihnutému povrchu, ktorý zadržiava vzduch. Preto nie je správne zameniť vyššie GSM za presný údaj o tepelnej izolácii.</p>
<p>Pri nákupe obliečok kontrolujte zloženie, väzbu, rozmery po praní, spôsob zapínania a ošetrovací štítok. Praktický postup prania rozoberá článok <a href="/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou">ako správne prať obliečky</a>. Ak porovnávate prírodné materiály, pomôže aj sprievodca <a href="/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost">vlastnosťami bavlny</a>.</p>

<h2>Gramáž trička: ľahký úplet nie je automaticky nekvalitný</h2>
<p>Pri tričkách ovplyvňuje GSM najmä pocit v ruke, splývavosť a mieru štruktúry. Ľahký úplet môže byť zámerne navrhnutý na horúce počasie, šport alebo vrstvenie. Vyššia gramáž býva pevnejšia, menej kopíruje telo a môže lepšie niesť niektoré potlače. Zároveň však pridáva hmotnosť, môže pôsobiť teplejšie a po praní schne pomalšie.</p>
<p>Priehľadnosť neurčuje iba gramáž. Tmavá farba, typ priadze a tesnosť pletenia môžu spôsobiť, že dve tričká s rovnakým GSM prepúšťajú svetlo rozdielne. Podobne ani odolnosť nie je priamo úmerná číslu: zle upletené ťažké tričko sa môže skrútiť vo švoch, zatiaľ čo kvalitný ľahší úplet si pri správnej starostlivosti drží tvar.</p>
<p>Pred nákupom látku jemne natiahnite proti svetlu, skontrolujte návrat do pôvodného tvaru, rovnosť bočných švov a povrch bez riedkych miest. Pri zmesi vlákien si prečítajte zloženie. Rozdiely medzi savosťou a schnutím prírodných a syntetických vlákien vysvetľujú články <a href="/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost">čo je bavlna</a> a <a href="/n/co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie">čo je mikrovlákno</a>.</p>

<h2>Ako vybrať gramáž podľa toho, kde textíliu používate</h2>
<p>Najpraktickejšia voľba nevznikne hľadaním najvyššieho čísla, ale pomenovaním podmienok. Uterák musí medzi použitiami vyschnúť, obliečky majú zodpovedať teplote spálne a tričko spôsobu nosenia. Zvážte aj kapacitu práčky, možnosť sušenia a to, či výrobok často prenášate.</p>
{decision_table}
<p>Ak nakupujete online a údaj o GSM chýba, pýtajte sa na plošnú hmotnosť alebo si porovnajte aspoň celkovú hmotnosť pri rovnakom rozmere a konštrukcii. Také porovnanie je len približné, pretože lem veľkej osušky tvorí menší podiel hmotnosti než lem malého uteráka.</p>

<h2>Ako zmerať GSM doma bez nesprávneho záveru</h2>
<p>Najpresnejší domáci odhad získate z rovnej textílie bez lemov a doplnkov. Odmerajte dĺžku a šírku v metroch, vypočítajte plochu a odvážte vzorku na váhe s vhodným rozlíšením. Celé tričko alebo obliečku možno odvážiť bez strihania, ale musíte započítať obe vrstvy, rukávy, golier, švy, zips či gombíky. Výsledok bude iba približný a pri zložitejšom strihu môže byť zavádzajúci.</p>
<p>Textília navyše prijíma a odovzdáva vlhkosť zo vzduchu. Bavlnený výrobok odvážený po sušení môže mať inú hmotnosť než ten istý kus po pobyte vo vlhkej kúpeľni. Profesionálne meranie preto používa definované podmienky a pripravené vzorky. Domáca váha je vhodná na orientačné porovnanie podobných kusov, nie na reklamáciu rozdielu niekoľkých gramov.</p>

<div style="border-left: 4px solid #111; padding: 14px 18px; margin: 22px 0; background: #fbfbfb;">
<h3 style="margin-top: 0;">Príklad s uterákom</h3>
<p>Uterák s rozmerom 50 × 100 cm má plochu približne 0,5 m². Ak váži 260 g vrátane lemov a etikety, hrubý prepočet je 260 ÷ 0,5 = 520 GSM. Skutočná plošná hmotnosť hlavnej froté časti môže byť trochu iná, pretože lem nemá rovnakú konštrukciu.</p>
</div>

<h2>Mení GSM spôsob prania a sušenia?</h2>
<p>GSM samo neurčuje teplotu, program ani povolené sušenie v sušičke. Tieto údaje vyčítate z ošetrovacích symbolov a zloženia. Ťažšia textília však mení praktickú prácu s náplňou. Hutné uteráky prijmú veľa vody, pri odstreďovaní sú ťažké a potrebujú priestor, aby sa prací roztok dostal medzi kusy a následne sa vypláchol.</p>
<p>Bubon preto neplňte iba podľa počtu uterákov alebo obliečok. Menovitá kapacita práčky sa vzťahuje na hmotnosť suchej náplne pre určený program, pričom jemné a špeciálne programy môžu povoľovať menej. Praktické rozdelenie náplne nájdete v článku <a href="/n/kolko-bielizne-dat-do-pracky-prakticka-kapacita-podla-uterakov-obliecok-a-sportu">koľko bielizne dať do práčky</a>.</p>
<p>Pri sušení rozprestrite plochu a vytvorte prúdenie vzduchu. Ťažký uterák zložený cez úzku tyč schne v mieste prekrytia pomaly. Obliečku pred zavesením rozmotajte a tričko vytvarujte podľa štítku, aby mokrá hmotnosť nenaťahovala golier. Vyššie otáčky môžu odstrániť viac vody, ale musia byť povolené pre danú konštrukciu.</p>

<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 22px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Poradie údajov pri starostlivosti</h2>
<ol>
<li><strong>Ošetrovací štítok:</strong> teplota, mechanické namáhanie, bielenie, sušenie a žehlenie.</li>
<li><strong>Zloženie a konštrukcia:</strong> bavlna, syntetika, zmes, froté, úplet alebo tkanina.</li>
<li><strong>Stav výrobku:</strong> farba, škvrny, poškodené švy a nánosy z predchádzajúceho prania.</li>
<li><strong>Gramáž:</strong> pomôže odhadnúť objem náplne, mokrú hmotnosť a čas schnutia.</li>
</ol>
</div>

<h2>Najčastejšie chyby pri porovnávaní gramáže</h2>
<ul>
<li><strong>Najvyššie GSM sa považuje za najvyššiu kvalitu.</strong> Číslo nehodnotí priadzu, švy, farbostálosť ani spracovanie.</li>
<li><strong>Porovnávajú sa rozdielne výrobky.</strong> Gramáž trička a uteráka nemá rovnaký funkčný význam.</li>
<li><strong>Zamieňa sa hrúbka a plošná hmotnosť.</strong> Nadýchaná štruktúra môže byť hrubá, ale nie mimoriadne ťažká; stlačená hustá textília môže pôsobiť tenšie a pritom vážiť viac.</li>
<li><strong>Ignoruje sa čas schnutia.</strong> Luxusne hutný uterák bez miesta na sušenie môže v praxi zapáchať skôr než ľahší model.</li>
<li><strong>Domáci prepočet zahŕňa lemy a doplnky.</strong> Výsledok potom nie je čistou gramážou hlavnej plochy.</li>
<li><strong>GSM nahrádza ošetrovací štítok.</strong> Plošná hmotnosť neurčuje bezpečnú teplotu ani chemické ošetrenie.</li>
</ul>

<h2>Odbornejší pohľad: ako plošnú hmotnosť merajú normy</h2>
<p>Medzinárodná norma ISO 3801 opisuje stanovenie hmotnosti tkanín na jednotku dĺžky a na jednotku plochy. Ide o metódu merania, nie o známku kvality konkrétneho uteráka alebo trička. Norma pomáha zjednotiť spôsob, akým sa pripraví a vyhodnotí vzorka, aby boli výsledky porovnateľnejšie.</p>
<p>Podobnú oblasť pokrýva ASTM D3776/D3776M, ktorá uvádza skúšobné možnosti stanovenia hmotnosti textílie na jednotku plochy. V laboratóriu záleží na odbere reprezentatívnej vzorky, presnej ploche, váhe aj podmienkach, v ktorých sa materiál nachádza. Preto sa domáci prepočet z celého hotového výrobku môže od deklarácie výrobcu mierne líšiť bez toho, aby jedno meranie automaticky dokazovalo chybu.</p>
<p>GINETEX zároveň vysvetľuje, že symboly údržby dávajú pokyny pre pranie, bielenie, sušenie, žehlenie a profesionálne ošetrovanie. To je dôležité oddelenie: ISO alebo ASTM pomáha určiť plošnú hmotnosť, zatiaľ čo ošetrovací štítok určuje bezpečný postup starostlivosti.</p>

<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; margin: 22px 0; background: #fbfbfb;">
<h2 style="margin-top: 0;">Použité odborné zdroje</h2>
<ul>
<li><a rel="noopener" href="https://www.iso.org/standard/9335.html" target="_blank">ISO 3801: Textiles — Woven fabrics — Determination of mass per unit length and mass per unit area</a></li>
<li><a rel="noopener" href="https://store.astm.org/standards/d3776" target="_blank">ASTM D3776/D3776M: Standard Test Methods for Mass Per Unit Area (Weight) of Fabric</a></li>
<li><a rel="noopener" href="https://www.ginetex.net/share/article/4201/care-symbols" target="_blank">GINETEX: Care symbols</a></li>
</ul>
</div>

<div style="border: 1px solid #dbe5de; border-radius: 8px; padding: 18px; margin: 24px 0; background: #f7fbf8;">
<h2 style="margin-top: 0;">Prací prostriedok vyberajte podľa textílie a štítku</h2>
<p>Gramáž pomáha nastaviť veľkosť náplne, no o vhodnom pracom prostriedku rozhoduje farba, materiál, znečistenie a pokyny výrobcu. Pri hutných kusoch je zvlášť dôležité neprekročiť dávku a ponechať v bubne priestor na dôkladný oplach.</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">Prací gél hypoalergénny z Marseillského mydla 1L</h3>
<p>Tekutý prací prostriedok na bežnú domácu starostlivosť používajte vždy podľa etikety produktu a ošetrovacieho štítku textílie. Dávku prispôsobte tvrdosti vody, znečisteniu a veľkosti suchej náplne.</p>
<p><strong>Dôležitá hranica:</strong> označenie produktu nenahrádza kontrolu materiálu, stálofarebnosti ani individuálnej citlivosti. Pri novom alebo citlivom kuse má prednosť štítok a skúška vhodného postupu.</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; background: #111; color: #fff; text-decoration: none;" href="/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l">Pozrieť produkt</a></p>
</div>
</div>

<div style="border: 1px solid #e6ded2; border-radius: 8px; padding: 18px; margin: 24px 0; background: #fffaf5;">
<h2 style="margin-top: 0;">Porovnajte pracie gély podľa typu bielizne</h2>
<p>Pri výbere zohľadnite materiál, farbu, teplotu povolenú štítkom aj to, či periete ľahké tričká, objemné obliečky alebo hutné uteráky. Správny výsledok stojí na vhodnom produkte, primeranej dávke, nepreplnenom bubne a dobrom oplachu.</p>
<div style="border: 1px solid #e5e5e5; border-radius: 8px; padding: 16px; background: #fff; margin: 14px 0;">
<h3 style="margin-top: 0;">Pracie gély</h3>
<p>V kategórii nájdete pracie gély pre rôzne potreby domácnosti. Pred použitím si prečítajte etiketu konkrétneho produktu a porovnajte ju s ošetrovacím štítkom bielizne.</p>
<p><a style="display: inline-block; padding: 11px 16px; border-radius: 6px; border: 1px solid #111; color: #111; text-decoration: none;" href="/c/vevo-home-care/pranie/praci-gel">Pozrieť pracie gély</a></p>
</div>
</div>

<h2>Súvisiace návody na VEVO</h2>
<ul>
<li><a href="/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost">Čo je bavlna: vlastnosti, výhody, nevýhody a starostlivosť</a></li>
<li><a href="/n/co-je-mikrovlakno-vyhody-nevyhody-savost-a-pranie">Čo je mikrovlákno: výhody, nevýhody, savosť a pranie</a></li>
<li><a href="/n/ako-prat-uteraky-rady-a-tipy-na-ciste-a-maekke-uteraky">Ako prať uteráky: rady pre čisté a mäkké uteráky</a></li>
<li><a href="/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou">Ako správne prať obliečky</a></li>
<li><a href="/n/kolko-bielizne-dat-do-pracky-prakticka-kapacita-podla-uterakov-obliecok-a-sportu">Koľko bielizne dať do práčky</a></li>
</ul>

<h2>FAQ: gramáž látky a GSM</h2>
<h3>Čo znamená 500 GSM pri uteráku?</h3>
<p>Znamená to, že jeden štvorcový meter danej textílie má hmotnosť približne 500 gramov. Neznamená to, že každý uterák váži 500 gramov; celková hmotnosť závisí od jeho rozmeru, lemov a doplnkov.</p>
<h3>Je uterák s vyšším GSM vždy savejší?</h3>
<p>Nie vždy. Vyššia gramáž často prináša viac materiálu, no savosť mení druh vlákna, slučková konštrukcia, povrchová úprava aj nánosy z prania. Dôležité je tiež, ako rýchlo uterák medzi použitiami vyschne.</p>
<h3>Aká gramáž uteráka je najlepšia?</h3>
<p>Pre bežnú kúpeľňu býva praktická stredná gramáž, ale správna voľba závisí od vetrania, frekvencie používania, priestoru na sušenie a osobného pocitu. Na cestovanie sa často hodí ľahší kus, na plnší hotelový pocit hutnejší.</p>
<h3>Aké GSM má kvalitné tričko?</h3>
<p>Kvalitu nemožno určiť jedným číslom. Ľahké aj ťažké tričko môže byť kvalitné, ak má vhodnú priadzu, rovnomernú pleteninu, pevné švy, dobrú stálofarebnosť a konštrukciu zodpovedajúcu účelu.</p>
<h3>Je GSM to isté ako počet nití?</h3>
<p>Nie. GSM vyjadruje hmotnosť na jednotku plochy. Počet nití opisuje hustotu nití v tkanine podľa použitej metodiky. Ani jeden údaj sám osebe nevystihuje celkovú kvalitu obliečok.</p>
<h3>Určuje gramáž teplotu prania?</h3>
<p>Nie. Teplotu, program, bielenie, sušenie a žehlenie určuje ošetrovací štítok a materiál. Gramáž pomáha skôr odhadnúť objem náplne, množstvo zadržanej vody a potrebný čas sušenia.</p>
""".strip()


def article_hrefs(markup):
    return re.findall(r'href="([^"]+)"', markup)


def preflight_links(article):
    headers = {"User-Agent": "Codex VEVO batch 39 test link preflight"}
    rows = []
    target_url = f"{BASE_URL}/n/{article['link']}"
    try:
        response = requests.get(
            target_url, timeout=30, allow_redirects=True, headers=headers
        )
        rows.append(
            {
                "url": target_url,
                "kind": "target_slug_precheck",
                "status": response.status_code,
                "final_url": response.url,
                "ok": response.status_code == 404,
                "error": None,
            }
        )
    except Exception as exc:
        rows.append(
            {
                "url": target_url,
                "kind": "target_slug_precheck",
                "status": None,
                "final_url": None,
                "ok": False,
                "error": str(exc),
            }
        )

    seen = set()
    for href in article_hrefs(article["long"]):
        url = urljoin(BASE_URL, href)
        if url in seen:
            continue
        seen.add(url)
        try:
            response = requests.get(
                url, timeout=30, allow_redirects=True, headers=headers
            )
            rows.append(
                {
                    "url": url,
                    "kind": "article_link",
                    "status": response.status_code,
                    "final_url": response.url,
                    "ok": 200 <= response.status_code < 400,
                    "error": None,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "url": url,
                    "kind": "article_link",
                    "status": None,
                    "final_url": None,
                    "ok": False,
                    "error": str(exc),
                }
            )

    return {
        "checked_count": len(rows),
        "failure_count": sum(1 for row in rows if not row["ok"]),
        "links": rows,
    }


def main():
    candidate_titles = [
        line.strip()
        for line in CANDIDATES_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if candidate_titles != [TITLE]:
        raise SystemExit("Batch 39 candidate file does not exactly match the article title")
    if not 120 <= len(META_DESCRIPTION) <= 165:
        raise SystemExit(
            f"Meta description length must be 120-165 characters, got {len(META_DESCRIPTION)}"
        )

    long = render_article()
    for value in (TITLE, META_DESCRIPTION, SHORT_DESCRIPTION, long):
        hits = FORBIDDEN_PUBLIC_RE.findall(value)
        if hits:
            raise SystemExit(f"Forbidden public wording found: {hits}")

    articles = [
        {
            "title": TITLE,
            "title_tag": TITLE,
            "description": META_DESCRIPTION,
            "short": SHORT_DESCRIPTION,
            "long": long,
            "date_posted": PUBLISH_DATE,
            "time_posted": "12:00",
            "active": True,
            "link": SLUG,
            "commenting": False,
        }
    ]
    ARTICLES_FILE.parent.mkdir(parents=True, exist_ok=True)
    ARTICLES_FILE.write_text(
        json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    report = preflight_links(articles[0])
    PREFLIGHT_FILE.parent.mkdir(parents=True, exist_ok=True)
    PREFLIGHT_FILE.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "article_count": len(articles),
                "output": str(ARTICLES_FILE),
                **report,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    if report["failure_count"]:
        raise SystemExit("Batch 39 link preflight failed")


if __name__ == "__main__":
    main()

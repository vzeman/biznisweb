import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH_DATE = "2025-09-26"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-26-2026-06-16-articles.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-26-laundry-process-clean-urls.xls"
HELPERS_PATH = Path("content/VEVO_CONTENT/imports/build_batch_21_material_guides.py")


spec = importlib.util.spec_from_file_location("batch21_helpers", HELPERS_PATH)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


ARTICLES = [
    {
        "title": "Ako funguje prací gél: tenzidy, enzýmy, pH a dávkovanie pri bežnom praní",
        "short": "Prací gél nečistí tým, že bielizeň iba prevonia. Pomáha uvoľniť mastnotu, pot a špinu z vlákien, udržať ich vo vode a opláchnuť ich z textilu. Rozhoduje dávka, tvrdosť vody, typ škvrny, teplota a čas.",
        "keywords": "ako funguje prací gél, čo sú tenzidy, enzýmy v pracom géli, pH pracieho prostriedku, dávkovanie pracieho gélu, prečo nepoužiť veľa gélu",
        "quick_title": "Rýchla odpoveď: čo robí prací gél",
        "quick": [
            "<strong>Tenzidy</strong> pomáhajú oddeliť mastnotu a špinu od textilu a dostať ju do pracieho kúpeľa.",
            "<strong>Enzýmy</strong> cielia na konkrétne typy škvŕn, napríklad bielkoviny, škroby alebo tuky, podľa zloženia produktu.",
            "<strong>pH a tvrdosť vody</strong> menia účinnosť prania aj to, koľko prípravku dáva zmysel použiť.",
            "<strong>Viac gélu nie je automaticky lepšie.</strong> Prebytok sa horšie oplachuje a môže prispieť k lepkavému pocitu alebo zápachu.",
        ],
        "intro": [
            "Keď bielizeň po praní nevonia alebo ostane šedá, veľa ľudí automaticky pridá viac pracieho gélu. Niekedy to krátkodobo pomôže, ale často sa tým problém zhorší. Prací gél musí mať priestor, vodu, čas a správne dávkovanie. Ak je bubon preplnený alebo je vody málo vzhľadom na dávku, zvyšky prostriedku ostanú v textílii.",
            "Praktické otázky typu ako funguje prací gél, čo sú tenzidy v pracom prostriedku, čo robia enzýmy v pracom géli alebo ako dávkovať prací gél majú spoločný základ: pranie je chemicko-mechanický proces. Nestačí silná vôňa. Treba odstrániť pot, maz, prach a škvrny z vlákien a následne ich dostať preč z bubna.",
            "Pri bežnom praní rozhoduje aj typ textilu. Uteráky, športová syntetika, posteľná bielizeň a jemné tričká majú rozdielnu savosť, objem a mieru zašpinenia. Preto dávkovanie podľa oka nie je ideálne. Lepšie je sledovať odporúčanie výrobcu, tvrdosť vody, veľkosť náplne a to, či ide o bežné nosenie alebo silné znečistenie.",
        ],
        "rows": [
            ("Tenzidy", "uvoľňujú a rozptyľujú mastnotu a špinu", "pomáhajú potu, mazu a prachu odísť z vlákna"),
            ("Enzýmy", "rozkladajú vybrané typy škvŕn", "nie každý produkt ani každá škvrna funguje rovnako"),
            ("pH", "ovplyvňuje podmienky prania", "citlivé textílie a pokožka potrebujú rozumný oplach"),
            ("Dávkovanie", "musí sedieť vode, náplni a špine", "prebytok gélu môže ostávať v bielizni"),
        ],
        "steps": [
            "Najprv určite veľkosť dávky: poloprázdny bubon, bežná dávka a preplnený bubon sa nedávkujú rovnako.",
            "Skontrolujte mieru znečistenia. Potené športové veci, pracovné oblečenie a posteľná bielizeň po chorobe nie sú rovnaká situácia.",
            "Pri opakovanom zápachu nepridávajte hneď viac gélu. Skúste menšiu dávku, dlhší program alebo extra oplach.",
            "Pri citlivej pokožke sledujte nielen parfumáciu, ale aj to, či sa prostriedok dôkladne vypláchne.",
            "Ak bielizeň ostáva tvrdá, lepkavá alebo má mapy, riešte dávkovanie, tvrdosť vody, čistotu zásobníka a preplnenie bubna.",
        ],
        "decision_rows": [
            ("Bežné tričká", "primeraná dávka, bežný program", "nepreplniť bubon"),
            ("Športová syntetika", "menej zvyškov, dobrý oplach", "pozor na avivážový film"),
            ("Uteráky", "dostatok priestoru a oplach", "zvyšky znižujú savosť"),
            ("Silné škvrny", "predošetrenie a vhodný program", "samotné navýšenie gélu nemusí stačiť"),
        ],
        "mistakes": [
            "Dávkovať podľa vône namiesto tvrdosti vody, veľkosti náplne a špiny.",
            "Použiť dvojnásobok gélu pri krátkom programe.",
            "Riešiť zápach ďalšou parfumáciou bez odstránenia potu a zvyškov prostriedku.",
            "Prať veľké uteráky alebo obliečky v preplnenom bubne.",
            "Nečistiť zásobník, v ktorom sa hromadí gél a aviváž.",
        ],
        "expert": "Tenzidy znižujú povrchové napätie a pomáhajú vode pracovať s mastnotou a nečistotami. Enzýmy sú biologické katalyzátory a v pracích produktoch sa používajú preto, že dokážu cieliť na konkrétne typy nečistôt. Prakticky to znamená, že výsledok nie je len o množstve gélu, ale o vhodnom produkte, správnej dávke, vode, teplote, čase a mechanike práčky.",
        "sources": [
            ("EPA: Safer Choice Criteria for Surfactants", "https://www.epa.gov/saferchoice/safer-choice-criteria-surfactants"),
            ("PubMed Central: enzyme-containing washing products", "https://pmc.ncbi.nlm.nih.gov/articles/PMC1796038/"),
            ("EPA: Safer Choice Standard", "https://www.epa.gov/saferchoice/standard"),
        ],
        "related": [
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
            ("Ako kombinovať prací gél a parfum do prania", "/n/ako-kombinovat-praci-gel-a-parfum-do-prania"),
            ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
        ],
        "faq": [
            ("Je lepšie dať viac pracieho gélu?", "Nie automaticky. Pri prebytku sa prostriedok horšie vyplachuje a môže zhoršiť pocit z bielizne."),
            ("Čo robia enzýmy v pracom géli?", "Pomáhajú cieliť na niektoré typy škvŕn. Výsledok však závisí od typu škvrny, teploty, času a zloženia produktu."),
            ("Prečo bielizeň po praní stále nevonia?", "Často nejde o málo vône, ale o pot, vlhkosť, zvyšky gélu, špinavú práčku alebo pomalé sušenie."),
        ],
    },
    {
        "title": "Predpieranie v práčke: kedy má zmysel a kedy len míňa vodu, čas a prací prostriedok",
        "short": "Predpieranie má zmysel pri skutočne špinavej bielizni, blate, pracovných veciach, kuchynských textíliách alebo detských nehodách. Pri bežnom nosení často stačí správny program, predbežné ošetrenie škvrny a primeraná dávka gélu.",
        "keywords": "predpieranie v práčke, kedy použiť predpieranie, predpierka na blato, predpieranie detského oblečenia, predpieranie posteľnej bielizne, krátky program alebo predpieranie",
        "quick_title": "Rýchla odpoveď: kedy zapnúť predpieranie",
        "quick": [
            "<strong>Zapnite ho pri reálnej špine.</strong> Blato, pracovný prach, zvyšky jedla, pelech alebo šport po daždi sú dobrý dôvod.",
            "<strong>Nepoužívajte ho zo zvyku.</strong> Bežné tričká po jednom nosení ho väčšinou nepotrebujú.",
            "<strong>Škvrnu často ošetrite lokálne.</strong> Predpieranie nenahrádza cielené predčistenie mastnoty, krvi alebo pigmentov.",
            "<strong>Pri jemných veciach opatrne.</strong> Dlhší proces znamená viac mechaniky, vody a času.",
        ],
        "intro": [
            "Predpieranie je funkcia, ktorú veľa práčok ponúka, ale mnoho domácností ju používa buď príliš často, alebo vôbec. Správne nastavené predpieranie vie pomôcť pri blate, pracovnom oblečení, detských nehodách, kuchynských utierkach alebo textíliách po domácich miláčikoch. Pri bežnom tričku po kancelárii je však často zbytočné.",
            "Ak hľadáte kedy použiť predpieranie v práčke, pýtajte sa najprv na typ špiny. Je to voľná špina, ktorú treba odplaviť pred hlavným praním? Alebo ide o konkrétnu škvrnu, ktorú treba lokálne ošetriť? Predpieranie pomáha najmä pri objeme špiny, nie pri každej škvrne.",
            "Dôležité je aj dávkovanie. Pri predpierke sa neopláca nasypať alebo naliať veľa prostriedku bez rozmýšľania. Ak zvolíte dlhší proces, musíte dať bielizni priestor, správne množstvo prípravku a následný dobrý oplach.",
        ],
        "rows": [
            ("Blato a piesok", "najprv mechanicky odstrániť, potom predpierka podľa potreby", "zníži sa šírenie špiny v hlavnom praní"),
            ("Pracovné oblečenie", "predpieranie môže mať zmysel", "najmä pri prachu, hline a pote"),
            ("Bežné tričká", "väčšinou bez predpierania", "stačí vhodný program a dávka"),
            ("Jemné textílie", "radšej lokálne a šetrne", "dlhá mechanika môže zhoršiť povrch"),
        ],
        "steps": [
            "Najprv vytraste alebo zotrite voľnú špinu. Práčka nemá slúžiť ako nádoba na blato.",
            "Škvrny predčistite podľa typu. Mastnota, krv, čokoláda a pigmenty potrebujú odlišný prístup.",
            "Predpieranie zapnite iba pri dávke, ktorá je naozaj silnejšie znečistená.",
            "Neplňte bubon nadoraz. Predpieranie potrebuje pohyb a vodu, inak sa špina iba presúva.",
            "Po praní skontrolujte, či bielizeň nezostala cítiť po vlhkosti alebo zvyškoch gélu.",
        ],
        "decision_rows": [
            ("Detské nohavice od blata", "áno, po vytrasení blata", "predpierka pomôže odplaviť voľnú špinu"),
            ("Košeľa po jednom nosení", "nie", "bežný program je efektívnejší"),
            ("Utierky od kuchyne", "niekedy", "pri mastnote je dôležité predčistenie a vhodný program"),
            ("Pelech alebo textílie po zvieratách", "často áno", "najprv odstrániť chlpy a hrubú špinu"),
        ],
        "mistakes": [
            "Zapínať predpieranie pri každej dávke zo zvyku.",
            "Nahradiť predčistenie škvrny dlhším programom.",
            "Prať blato bez vytrasenia priamo v práčke.",
            "Dávať príliš veľa gélu do predpierky aj hlavného prania.",
            "Miešať silno špinavé pracovné veci s jemnou bielizňou.",
        ],
        "expert": "Predpieranie zvyšuje čas, spotrebu vody a mechanické pôsobenie na textil. Preto dáva zmysel vtedy, keď hlavné pranie potrebuje odľahčiť od hrubej alebo objemovej špiny. Pri škvrnách je často účinnejšie cielené predbežné ošetrenie a správny hlavný program než univerzálne predpieranie všetkého.",
        "sources": [
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
            ("Energy Star: Clothes washers", "https://www.energystar.gov/products/clothes_washers"),
            ("FTC: Care Labeling Rule guidance", "https://www.ftc.gov/business-guidance/resources/clothes-captioning-complying-care-labeling-rule"),
        ],
        "related": [
            ("Ako vyčistiť bubon práčky po praní pelechu, topánok alebo pracovných vecí", "/n/ako-vycistit-bubon-pracky-po-prani-pelechu-topanok-alebo-pracovnych-veci"),
            ("Ako odstrániť hlinu z pracovného trička po záhradkárčení", "/n/ako-vyprat-pracovne-tricko-po-zahradkarceni-od-hliny-a-potu"),
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
        ],
        "faq": [
            ("Treba predpieranie pri každom praní?", "Nie. Pri bežnom nosení je zvyčajne zbytočné a iba predlžuje proces."),
            ("Pomôže predpieranie na mastné škvrny?", "Niekedy pomôže, ale mastnotu je lepšie najprv cielene predčistiť podľa materiálu."),
            ("Je predpieranie vhodné na detské oblečenie?", "Pri blate alebo väčšom znečistení áno. Pri bežnom detskom tričku stačí šetrný hlavný program."),
        ],
    },
    {
        "title": "Otáčky pri odstreďovaní: ako ovplyvňujú vlhkosť, krčenie a opotrebovanie oblečenia",
        "short": "Vyššie otáčky vyžmýkajú viac vody, ale môžu zvýšiť krčenie, trenie a namáhanie jemných alebo elastických textílií. Najlepšie otáčky závisia od materiálu, objemu dávky a spôsobu sušenia.",
        "keywords": "otáčky pri odstreďovaní, koľko otáčok na pranie, 800 alebo 1200 otáčok, odstreďovanie oblečenia, krčenie po praní, jemná bielizeň otáčky",
        "quick_title": "Rýchla odpoveď k otáčkam",
        "quick": [
            "<strong>Vyššie otáčky znamenajú menej vody v bielizni.</strong> Sušenie potom trvá kratšie.",
            "<strong>Nižšie otáčky sú šetrnejšie.</strong> Pomáhajú pri jemných látkach, viskóze, vlne, elastane a oblečení s potlačou.",
            "<strong>Veľké kusy potrebujú rovnováhu.</strong> Obliečky a uteráky sa pri zlom rozložení môžu zle odstreďovať.",
            "<strong>Otáčky neriešia čistotu.</strong> Čistota vzniká počas prania a oplachu, nie až pri odstreďovaní.",
        ],
        "intro": [
            "Otáčky pri odstreďovaní patria medzi nastavenia, ktoré ľudia často nechávajú automaticky. Pritom výrazne ovplyvňujú, ako mokrá bielizeň vyjde z práčky, ako dlho bude schnúť a ako veľmi sa pokrčí alebo mechanicky namáha. Nie je pravda, že najvyššie otáčky sú vždy najlepšie.",
            "Ak hľadáte koľko otáčok nastaviť pri praní, odpoveď závisí od materiálu. Uteráky a pevná bavlna znesú viac, jemná blúzka, viskózové šaty, vlnený sveter alebo elastické športové oblečenie potrebujú šetrnejší prístup. Pri vysokých otáčkach ide najmä o mechaniku, nie o teplotu.",
            "Prakticky rozhoduje aj sušenie. Ak sušíte v byte, vyššie otáčky môžu skrátiť schnutie a znížiť vlhkosť v priestore. Ak však periete jemné textílie, príliš silné odstreďovanie môže spôsobiť krčenie, deformáciu alebo horší vzhľad povrchu.",
        ],
        "rows": [
            ("400-600 ot./min", "veľmi šetrné odstreďovanie", "vlna, jemné kusy, ručné programy podľa štítku"),
            ("800 ot./min", "kompromis šetrnosti a vlhkosti", "blúzky, tričká, zmesi, citlivejšie textílie"),
            ("1000-1200 ot./min", "bežné silnejšie odstreďovanie", "bavlna, obliečky, menej citlivé kusy"),
            ("1400+ ot./min", "veľmi suchý výstup, viac mechaniky", "iba ak to textil a práčka zvládajú"),
        ],
        "steps": [
            "Najprv skontrolujte štítok a program. Jemný program často automaticky zníži odstreďovanie.",
            "Pri novom alebo citlivom kúsku zvoľte radšej nižšie otáčky a sledujte výsledok.",
            "Pri veľkých kusoch nepreplňte bubon, aby sa dávka vedela vyvážiť.",
            "Ak sušíte v byte, skúste nájsť kompromis medzi nižšou vlhkosťou a šetrnosťou k textilu.",
            "Keď je bielizeň veľmi pokrčená, problém nemusí byť len žehlenie, ale aj vysoké otáčky a preplnenie.",
        ],
        "decision_rows": [
            ("Vlnený sveter", "nízke otáčky alebo program vlna", "chráni tvar a povrch"),
            ("Viskózové šaty", "nižšie otáčky", "mokrý materiál sa môže deformovať"),
            ("Uteráky", "vyššie podľa štítku a práčky", "rýchlejšie schnú, ale potrebujú priestor"),
            ("Obliečky", "stredné až vyššie podľa materiálu", "nepreplniť bubon a rozložiť veľké kusy"),
        ],
        "mistakes": [
            "Používať najvyššie otáčky na každý materiál.",
            "Myslieť si, že odstreďovanie nahradí správny oplach.",
            "Preplniť bubon veľkými obliečkami a čudovať sa vlhkému výsledku.",
            "Silno odstreďovať elastanové alebo jemné kusy s potlačou.",
            "Riešiť krčenie iba žehlením, nie nastavením prania.",
        ],
        "expert": "Odstreďovanie je mechanické odstraňovanie vody z textilu. Z energetického hľadiska môže suchší výstup z práčky znížiť čas sušenia, ale z textilného hľadiska treba rešpektovať konštrukciu a citlivosť výrobku. Preto je vhodné pracovať s kompromisom: pevnejšie textílie znesú viac otáčok, jemné a elastické kúsky menej.",
        "sources": [
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
            ("Energy Star: Clothes washers", "https://www.energystar.gov/products/clothes_washers"),
            ("GINETEX: Labelling", "https://www.ginetex.net/GB/labelling/labelling.asp"),
        ],
        "related": [
            ("Prečo sa oblečenie zrazí po praní", "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"),
            ("Čo je viskóza: vlastnosti, krčivosť, zrážanie a starostlivosť", "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
        ],
        "faq": [
            ("Je lepšie 800 alebo 1200 otáčok?", "Pri jemnejších veciach skôr 800, pri pevnej bavlne a uterákoch často 1200. Rozhoduje štítok a materiál."),
            ("Poškodzujú vysoké otáčky oblečenie?", "Pri citlivých, elastických alebo jemných kusoch môžu zvýšiť mechanické namáhanie a krčenie."),
            ("Prečo je bielizeň po odstreďovaní stále mokrá?", "Príčinou môže byť preplnený bubon, zlé rozloženie veľkých kusov, nízke otáčky alebo problém s odtokom či filtrom."),
        ],
    },
    {
        "title": "Preplnená práčka: prečo sa bielizeň nevyperie, neopláchne a zapácha",
        "short": "Preplnená práčka nešetrí čas ani peniaze, ak sa bielizeň nevyperie a musí ísť znova do bubna. Textil potrebuje priestor na pohyb, vodu, prací roztok a oplach. Inak ostáva pot, špina aj zvyšky gélu vo vláknach.",
        "keywords": "preplnená práčka, koľko bielizne do práčky, práčka plná nadoraz, bielizeň sa nevyperie, bielizeň zapácha po praní, zle opláchnutá bielizeň",
        "quick_title": "Rýchla odpoveď: čo spôsobí preplnený bubon",
        "quick": [
            "<strong>Bielizeň sa menej hýbe.</strong> Mechanika prania je slabšia a špina sa horšie uvoľní.",
            "<strong>Oplach je horší.</strong> Zvyšky gélu a špiny môžu ostať v textílii.",
            "<strong>Veľké kusy sa zle rozložia.</strong> Obliečky, uteráky a deky môžu vytvoriť ťažké zhluky.",
            "<strong>Zápach sa vracia.</strong> Najmä pri športovej syntetike, uterákoch a posteľnej bielizni.",
        ],
        "intro": [
            "Preplniť práčku je lákavé. Bielizeň sa nahromadí, času je málo a človek chce urobiť jednu veľkú dávku namiesto dvoch menších. Problém je, že práčka neperie len tým, že sa bubon otočí. Textil potrebuje priestor, vodu, prací roztok a trenie medzi kusmi v rozumnej miere.",
            "Ak je práčka plná nadoraz, bielizeň sa prevaľuje ako ťažká masa. Voda a prací gél sa nedostanú rovnomerne všade, špina sa horšie uvoľní a oplach nemusí vytiahnuť zvyšky prostriedku. Výsledkom je bielizeň, ktorá vyzerá mokro čistá, ale po vysušení cítiť pot, zatuchnutie alebo lepkavosť.",
            "Praktické otázky ako koľko bielizne do práčky, prečo sa bielizeň nevyperie, prečo oblečenie smrdí po praní alebo preplnený bubon a zvyšky gélu vedú k jednej praktickej zásade: práčka má mať pracovný priestor. Plný bubon nie je to isté ako efektívny bubon.",
        ],
        "rows": [
            ("Málo priestoru", "textil sa nehýbe voľne", "slabšie pranie a viac pokrčenia"),
            ("Nerovnomerný prací roztok", "gél sa nedostane všade", "špina a pot ostávajú v zhlukoch"),
            ("Slabší oplach", "zvyšky ostávajú vo vláknach", "tvrdý, lepkavý alebo zapáchajúci výsledok"),
            ("Horšie odstreďovanie", "veľké kusy sa zle vyvážia", "bielizeň je mokrejšia a schne dlhšie"),
        ],
        "steps": [
            "Pri bežnej bielizni nechajte v bubne voľný priestor na pohyb textilu.",
            "Veľké kusy ako obliečky, uteráky a deky nemiešajte do dávky, ktorá je už nadoraz.",
            "Pri športovej syntetike perte radšej menšiu dávku, aby sa vypláchol pot a maz.",
            "Ak bielizeň po praní zapácha, skúste menšiu náplň a extra oplach skôr než viac gélu.",
            "Po praní vyberte dávku hneď. Preplnená mokrá bielizeň v práčke zatuchne rýchlejšie.",
        ],
        "decision_rows": [
            ("Uteráky", "menej kusov v dávke", "potrebujú vodu a dobrý oplach"),
            ("Obliečky", "zapnúť zipsy, nepreplniť", "veľké kusy sa môžu zamotať"),
            ("Športové oblečenie", "menšia dávka, dobrý oplach", "pot a maz sa musia vypláchnuť"),
            ("Jemné tričká", "oddeliť od ťažkých kusov", "nižšie trenie a menej deformácie"),
        ],
        "mistakes": [
            "Doplniť posledné miesto v bubne uterákom len preto, že sa ešte zmestí.",
            "Použiť viac gélu, aby zvládol preplnenú dávku.",
            "Miešať obliečky, uteráky, rifle a jemné tričká v jednej ťažkej náplni.",
            "Ignorovať opakovaný zápach a stále prať rovnako veľké dávky.",
            "Nechať veľkú mokrú dávku v práčke po skončení programu.",
        ],
        "expert": "Moderné práčky pracujú s optimalizovanou spotrebou vody a energie. To je výhoda, ale zároveň to znamená, že preplnenie bubna môže znížiť účinnosť prania a oplachu. Mechanika, voda a prací roztok musia mať priestor. Pri veľmi veľkej náplni sa zvyšuje riziko nerovnomerného vyprania, zvyškov prostriedku a dlhšieho sušenia.",
        "sources": [
            ("Energy Star: Clothes washers", "https://www.energystar.gov/products/clothes_washers"),
            ("Energy.gov: Laundry", "https://www.energy.gov/energysaver/laundry"),
        ],
        "related": [
            ("Prečo oblečenie zapácha po praní", "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
            ("Ako vyčistiť filter práčky, keď bielizeň zapácha", "/n/ako-vycistit-filter-pracky-ked-bielizen-zapacha-alebo-voda-odteka-pomaly"),
        ],
        "faq": [
            ("Ako zistím, že je práčka preplnená?", "Ak bielizeň nemá priestor na pohyb a bubon je natlačený nadoraz, je to priveľa. Pri veľkých kusoch nechajte ešte viac priestoru."),
            ("Pomôže viac pracieho gélu pri plnom bubne?", "Skôr nie. Prebytok gélu sa môže horšie opláchnuť a zhoršiť pach alebo lepkavosť."),
            ("Prečo je bielizeň po praní stále cítiť?", "Často ide o kombináciu preplnenia, zvyškov potu, nedostatočného oplachu, vlhkej práčky alebo pomalého sušenia."),
        ],
    },
    {
        "title": "Prečo je bielizeň po praní tvrdá alebo lepkavá: zvyšky gélu, dávkovanie a oplach",
        "short": "Tvrdá alebo lepkavá bielizeň po praní často znamená zvyšky pracieho prostriedku, aviváže, minerálov z tvrdej vody alebo nedostatočné opláchnutie. Riešením nebýva viac vône, ale lepšie dávkovanie, menšia náplň a čistá práčka.",
        "keywords": "bielizeň po praní tvrdá, bielizeň po praní lepkavá, zvyšky pracieho gélu v bielizni, zle opláchnutá bielizeň, priveľa gélu v práčke, tvrdá voda a bielizeň",
        "quick_title": "Rýchla odpoveď: tvrdá alebo lepkavá bielizeň",
        "quick": [
            "<strong>Najprv znížte dávku gélu.</strong> Veľa prostriedku neznamená lepší výsledok.",
            "<strong>Pridajte oplach.</strong> Ak textil lepí, škriabe alebo vonia príliš silno, môže v ňom niečo ostať.",
            "<strong>Neplňte bubon nadoraz.</strong> Zle opláchnutá bielizeň často vzniká pri preplnení.",
            "<strong>Skontrolujte tvrdú vodu a práčku.</strong> Zásobník, tesnenie a bubon môžu problém zhoršovať.",
        ],
        "intro": [
            "Bielizeň má byť po praní čistá, príjemná a po vysušení prirodzená na dotyk. Ak je tvrdá, lepkavá, drsná alebo pôsobí akoby mala na sebe film, netreba hneď pridávať aviváž alebo parfum. Najprv treba zistiť, čo v textile ostalo.",
            "Najčastejšie ide o zvyšky pracieho gélu, aviváže, minerály z tvrdej vody, preplnený bubon alebo slabý oplach. Problém môže byť aj v zásobníku práčky, kde sa hromadí starý gél a aviváž, alebo v tesnení, ktoré vracia zatuchnutý pach do bielizne.",
            "Praktické otázky ako prečo je bielizeň po praní tvrdá, prečo je bielizeň lepkavá, ako odstrániť zvyšky pracieho gélu z oblečenia alebo čo robiť pri zle opláchnutej bielizni majú praktické riešenie: menej prebytkov, viac priestoru, lepší oplach a suché skladovanie.",
        ],
        "rows": [
            ("Priveľa gélu", "prostriedok sa nevypláchne úplne", "textil môže lepiť alebo silno voňať"),
            ("Tvrdá voda", "minerály ovplyvňujú pranie aj pocit z textilu", "dávkovanie treba prispôsobiť vode"),
            ("Preplnený bubon", "voda sa nedostane rovnomerne všade", "horší oplach a viac zvyškov"),
            ("Zásobník práčky", "usadeniny sa vracajú do prania", "treba ho pravidelne čistiť"),
        ],
        "steps": [
            "Skúste jednu dávku vyprať s menším množstvom gélu a extra oplachom.",
            "Znížte náplň bubna, najmä pri uterákoch, obliečkach a športových veciach.",
            "Vyčistite zásobník na prací prostriedok a skontrolujte tesnenie práčky.",
            "Pri tvrdej vode sledujte odporúčanie dávkovania a porovnajte výsledok po úprave dávky.",
            "Bielizeň po praní rýchlo vyberte a úplne vysušte, aby sa tvrdosť nezmenila na zatuchnutie.",
        ],
        "decision_rows": [
            ("Tvrdé uteráky", "menej zvyškov, viac oplachu, nepreplniť", "zvyšky zhoršujú savosť aj pocit"),
            ("Lepkavé tričko", "znížiť dávku gélu a vyprať znova", "film môže dráždiť pokožku"),
            ("Silná vôňa po praní", "skontrolovať dávkovanie parfumu aj gélu", "vôňa nemá nahradiť oplach"),
            ("Šednúca biela bielizeň", "riešiť vodu, triedenie a dávkovanie", "nejde iba o slabý produkt"),
        ],
        "mistakes": [
            "Pridať aviváž na tvrdé uteráky bez riešenia zvyškov gélu.",
            "Použiť viac pracieho prostriedku, keď je bielizeň lepkavá.",
            "Prať obliečky a uteráky v preplnenom bubne.",
            "Ignorovať špinavý zásobník na prací prostriedok.",
            "Maskovať zatuchnutie silnou vôňou bez vyčistenia práčky.",
        ],
        "expert": "Tvrdosť vody je daná najmä obsahom rozpusteného vápnika a horčíka. V domácnosti sa prejavuje usadeninami aj tým, ako pracujú čistiace prostriedky. Pri praní však tvrdosť vody nie je jediný faktor. Pocit tvrdosti alebo lepkavosti často vzniká kombináciou minerálov, dávkovania, zvyškov prostriedku, nízkeho objemu vody v programe a preplneného bubna.",
        "sources": [
            ("USGS: Hardness of Water", "https://www.usgs.gov/water-science-school/science/hardness-water"),
            ("EPA: Safer Choice Label", "https://www.epa.gov/saferchoice/learn-about-safer-choice-label"),
            ("GINETEX: Labelling", "https://www.ginetex.net/GB/labelling/labelling.asp"),
        ],
        "related": [
            ("Tvrdá voda a pranie: prečo je bielizeň tvrdá, sivá a bez vône", "/n/tvrda-voda-a-pranie-preco-je-bielizen-tvrda-siva-a-bez-vone"),
            ("Ako vyčistiť zásobník práčky od usadenín gélu a aviváže", "/n/ako-vycistit-zasobnik-pracky-od-usadenin-pracieho-gelu-a-avivaze"),
            ("Ako prať bielu bielizeň, aby nezošedla a nezapáchala", "/n/ako-prat-bielu-bielizen-aby-nezosedla-a-nezapachala"),
        ],
        "faq": [
            ("Prečo je bielizeň po praní tvrdá?", "Často pre tvrdú vodu, zvyšky pracieho prostriedku, preplnenie bubna alebo slabý oplach."),
            ("Čo robiť, keď je oblečenie lepkavé?", "Skúste ho vyprať znova s menšou dávkou gélu, bez aviváže a s extra oplachom."),
            ("Pomôže parfum do prania na tvrdú bielizeň?", "Nie ako hlavné riešenie. Najprv treba vyriešiť zvyšky prostriedku, tvrdú vodu, oplach a sušenie."),
        ],
    },
]


def build_long(article):
    html = [
        f"<p><strong>Rýchla odpoveď:</strong> {article['short']}</p>",
        f"<p>Okrem hlavnej odpovede rozoberáme aj praktické situácie z domácnosti: <strong>{article['keywords']}</strong>. Dôležité je vysvetliť princíp jednoducho a potom ho preniesť do bežnej domácej rutiny: dávkovanie, veľkosť náplne, oplach, sušenie a výber vhodného produktu.</p>",
        helpers.quick_box(article["quick_title"], article["quick"]),
    ]
    html.extend(f"<p>{paragraph}</p>" for paragraph in article["intro"])
    html.append("<h2>Ako to funguje v praxi</h2>")
    html.append(helpers.table(["Faktor", "Čo sa deje", "Čo si všímať doma"], article["rows"]))
    html.append("<h2>Praktický postup krok za krokom</h2>")
    html.append("<ol>" + "".join(f"<li>{step}</li>" for step in article["steps"]) + "</ol>")
    html.append("<h2>Rýchle rozhodovanie podľa situácie</h2>")
    html.append(helpers.table(["Situácia", "Odporúčaný postup", "Prečo"], article["decision_rows"]))
    html.append("<h2>Najčastejšie chyby</h2>")
    html.append("<ul>" + "".join(f"<li>{item}</li>" for item in article["mistakes"]) + "</ul>")
    html.append("<h2>Odbornejší pohľad</h2>")
    html.append(f"<p>{article['expert']}</p>")
    html.append(helpers.sources(article["sources"]))
    html.append(helpers.recommendation())
    html.append(helpers.related(article["related"]))
    html.append("<h2>FAQ</h2>")
    for question, answer in article["faq"]:
        html.append(f"<h3>{question}</h3><p>{answer}</p>")
    return "\n".join(html)


def main():
    articles = []
    times = ["08:00:00", "08:12:00", "08:24:00", "08:36:00", "08:48:00"]
    for index, article in enumerate(ARTICLES):
        long = build_long(article)
        if re.search(r"\bCTA\b", long, re.IGNORECASE):
            raise SystemExit(f"Forbidden CTA wording in {article['title']}")
        if "Cena:" in long or re.search(r"\d+[,.]\d{1,2}\s*€", long):
            raise SystemExit(f"Fixed price wording in {article['title']}")
        if len(long) > 32700:
            raise SystemExit(f"XLS cell too long for {article['title']}: {len(long)}")
        articles.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long,
                "date_posted": BATCH_DATE,
                "time_posted": times[index],
                "active": 1,
                "link": helpers.slugify(article["title"]),
                "commenting": "none",
            }
        )

    hrefs = sorted({href for row in articles for href in re.findall(r'href="([^"]+)"', row["long"])})
    checks = []
    for href in hrefs:
        if href.startswith("/"):
            url = urljoin(BASE, href)
        elif href.startswith("http"):
            url = href
        else:
            continue
        response = requests.get(url, headers={"User-Agent": "Codex VEVO batch 26 link preflight"}, timeout=45, allow_redirects=True)
        checks.append((href, response.status_code, response.url))
        if response.status_code != 200:
            raise SystemExit(f"Link preflight failed: {href} -> {response.status_code} {response.url}")

    OUT_JSON.write_text(json.dumps(articles, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    book = xlwt.Workbook(encoding="utf-8")
    sheet = book.add_sheet("news")
    headers = ["title", "short", "long", "date_posted", "time_posted", "active", "link", "commenting"]
    for col, header in enumerate(headers):
        sheet.write(0, col, header)
    for row_index, article in enumerate(articles, start=1):
        for col, header in enumerate(headers):
            sheet.write(row_index, col, article[header])
    OUT_XLS.parent.mkdir(parents=True, exist_ok=True)
    book.save(str(OUT_XLS))

    print(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "article_count": len(articles),
                "json": str(OUT_JSON),
                "xls": str(OUT_XLS),
                "links_checked": len(checks),
                "lengths": {article["title"]: len(article["long"]) for article in articles},
                "slugs": [article["link"] for article in articles],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

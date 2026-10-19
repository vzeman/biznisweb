import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH_DATE = "2025-09-28"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-24-2026-06-16-articles.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-24-material-guides-clean-urls.xls"
HELPERS_PATH = Path("content/VEVO_CONTENT/imports/build_batch_21_material_guides.py")


spec = importlib.util.spec_from_file_location("batch21_helpers", HELPERS_PATH)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


ARTICLES = [
    {
        "title": "Čo je fleece: hrejivosť, žmolkovanie a starostlivosť pri praní",
        "short": "Fleece je mäkký česaný úplet, najčastejšie z polyesteru. Hreje, rýchlo schne a je ľahký, ale pri praní treba riešiť žmolkovanie, statiku a uvoľňovanie drobných vlákien.",
        "keywords": "čo je fleece, fleece materiál, ako prať fleece, fleece mikina pranie, fleece žmolkovanie, polyesterový fleece starostlivosť",
        "quick_title": "Rýchla odpoveď pre fleece mikiny a deky",
        "quick": [
            "<strong>Fleece je väčšinou polyesterový úplet s česaným povrchom.</strong> Preto je ľahký, mäkký, hrejivý a rýchlo schne.",
            "<strong>Perte ho naruby a bez drsných kusov.</strong> Zipsy, suché zipsy a uteráky môžu zhoršiť žmolkovanie.",
            "<strong>Nepreháňajte teplotu ani sušičku.</strong> Fleece nepotrebuje horúce pranie; dôležitejšie je dobré opláchnutie a vzdušné sušenie.",
            "<strong>Pri športe alebo turistike riešte pot včas.</strong> Vlhký fleece zatvorený v batohu začne rýchlo zapáchať.",
        ],
        "intro": [
            "Fleece poznáte z mikín, búnd, detských overalov, nákrčníkov, rukavíc, diek a outdoorových vrstiev. Je obľúbený preto, že je mäkký, relatívne ľahký, príjemne hreje a po praní zvyčajne schne rýchlejšie než hrubá bavlna.",
            "Najčastejšie ide o polyesterový materiál s česaným povrchom. Práve česaný povrch vytvára mäkký pocit, ale zároveň znamená vyššie riziko žmolkovania, zachytávania chlpov, statickej elektriny a uvoľňovania drobných vlákien pri praní.",
            "Ak hľadáte návod ako prať fleece mikinu, fleece deku alebo detský fleece overal, nezačínajte silným pracím programom. Začnite štítkom, otočením naruby, oddelením od zipsov a rozumnou dávkou pracieho gélu. Fleece má byť po praní čistý a mäkký, nie zlepený zvyškami prostriedku.",
        ],
        "property_rows": [
            ("Hrejivosť", "mäkký česaný povrch drží vzduch", "neutláčať v preplnenom bubne"),
            ("Rýchle schnutie", "polyesterová báza nezadrží veľa vody", "vybrať hneď po praní a sušiť vzdušne"),
            ("Žmolkovanie", "povrch trpí trením", "prať naruby, oddeliť od zipsov a suchých zipsov"),
            ("Statika", "syntetika môže elektrizovať", "nesušiť zbytočne horúco a nepresušovať"),
        ],
        "care_rows": [
            ("Fleece mikina", "Zapnúť zips, otočiť naruby, jemný program a nižšie otáčky.", "Chráni povrch pred trením a žmolkami."),
            ("Fleece deka", "Nepreplniť bubon, použiť primeranú dávku gélu a dobre vysušiť.", "Veľký objem potrebuje priestor na oplach."),
            ("Detský fleece overal", "Prať s podobne jemnými kusmi, bez aviváže pri funkčnej vrstve.", "Detské kúsky často trpia od piesku, blata a suchých zipsov."),
        ],
        "mistakes": [
            "Prať fleece spolu s uterákmi, rifľami, suchým zipsom alebo hrubými bundami.",
            "Použiť priveľa gélu a krátky oplach, po ktorom je povrch tuhší.",
            "Sušiť fleece horúco len preto, že ide o syntetiku.",
            "Nechať spotenú fleece mikinu zavretú v batohu alebo športovej taške.",
        ],
        "expert": "Fleece je praktický príklad toho, že materiál nie je len chemické vlákno, ale aj konštrukcia látky. Polyesterová báza môže byť odolná, no česaný povrch je citlivý na trenie. Pri praní preto chránite najmä povrch, nie iba samotné vlákno. Pri častom praní syntetických textílií má zmysel riešiť aj mechanické namáhanie a uvoľňovanie drobných vlákien.",
        "sources": [
            ("Textile Exchange: Other synthetics", "https://textileexchange.org/other-synthetics/"),
            ("Microbial odor profile of polyester and cotton clothes", "https://pmc.ncbi.nlm.nih.gov/articles/PMC4249026/"),
        ],
        "related": [
            ("Čo je polyester a ako ho prať, aby nezapáchal", "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"),
            ("Polyester vs bavlna: rozdiely pri nosení, praní a vôni", "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"),
            ("Akryl vs vlna: žmolkovanie, teplo, zápach a starostlivosť", "/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost"),
        ],
        "faq": [
            ("Ako prať fleece mikinu?", "Naruby, so zapnutým zipsom, s podobne jemnými kusmi a bez preplnenia bubna. Po praní ju hneď vyberte a sušte vzdušne."),
            ("Prečo fleece žmolkuje?", "Najčastejšie pre trenie pri nosení a praní. Pomáha pranie naruby, oddelenie od drsných kusov a šetrný program."),
            ("Môže ísť fleece do sušičky?", "Len ak to povoľuje štítok. Pri neistote je bezpečnejšie sušenie na vzduchu mimo radiátora."),
        ],
    },
    {
        "title": "Čo je softshell: vrstvy, membrána, impregnácia a správna starostlivosť",
        "short": "Softshell je funkčný viacvrstvový materiál na bundy a nohavice. Chráni pred vetrom a miernou vlhkosťou, ale pri praní nemá rád aviváž, preplnený bubon a zvyšky pracieho prostriedku.",
        "keywords": "čo je softshell, softshell materiál, softshell membrána, softshell pranie, softshell impregnácia, ako sa starať o softshell",
        "quick_title": "Rýchla odpoveď pre softshell",
        "quick": [
            "<strong>Softshell nie je jeden konkrétny materiál.</strong> Je to typ funkčnej textílie, ktorá môže kombinovať vonkajšiu vrstvu, pružnosť, membránu alebo vodoodpudivú úpravu.",
            "<strong>Pri praní vynechajte aviváž.</strong> Môže zanechať film a zhoršiť funkčné vlastnosti textilu.",
            "<strong>Impregnácia nie je náhrada prania.</strong> Rieši sa až na čistom a suchom softshelle, keď voda prestane tvoriť kvapky.",
            "<strong>Detský softshell neperte po každom blate.</strong> Malú špinu najprv riešte lokálne, aby sa materiál zbytočne nenamáhal.",
        ],
        "intro": [
            "Softshell sa používa na bundy, nohavice, detské outdoorové oblečenie, turistické kúsky a prechodné vrstvy. Ľudia ho kupujú preto, že je pružnejší než klasická nepremokavá bunda, príjemnejší pri pohybe a často lepšie znáša bežné nosenie v meste aj vonku.",
            "Pod slovom softshell sa však skrýva viac konštrukcií. Niektorý softshell má membránu, iný hlavne hustú tkaninu a vodoodpudivú úpravu. Preto sa nedá povedať jeden univerzálny postup pre všetky bundy. Vždy rozhoduje štítok, membrána, záter, zipsy, reflexné prvky a stav povrchu.",
            "Ak hľadáte čo je softshell, ako prať softshell bundu alebo kedy obnoviť impregnáciu softshellu, praktická odpoveď je jednoduchá: čistiť jemne, nepoužívať aviváž, dobre oplachovať a impregnáciu riešiť až vtedy, keď je naozaj potrebná.",
        ],
        "property_rows": [
            ("Vrstvy", "môže spájať vonkajšiu látku, membránu a vnútorný komfort", "prať podľa štítku konkrétneho kusu"),
            ("Pružnosť", "často obsahuje elastické zložky", "neprehrievať a nepoužívať agresívne odstreďovanie"),
            ("Vodoodpudivosť", "kvapky sa majú držať na povrchu", "aviváž a zvyšky gélu môžu výsledok zhoršiť"),
            ("Priedušnosť", "závisí od konštrukcie a čistoty vrstiev", "dôležitý je primeraný gél a dobrý oplach"),
        ],
        "care_rows": [
            ("Softshell bunda", "Zapnúť zipsy, otočiť naruby, jemný program a malé množstvo gélu.", "Chráni vonkajšiu vrstvu a membránu."),
            ("Softshell nohavice", "Najprv odstrániť zaschnuté blato, potom prať podľa štítku.", "Blato v bubne zvyšuje trenie."),
            ("Detský softshell", "Častejšie lokálne čistenie, pranie až pri zápachu alebo špine zvnútra.", "Znižuje zbytočné opotrebovanie."),
        ],
        "mistakes": [
            "Použiť aviváž, aby bol softshell mäkší.",
            "Impregnovať špinavú bundu namiesto prania.",
            "Prať softshell s uterákmi alebo rifľami.",
            "Sušiť na radiátore a poškodiť funkčné vrstvy teplom.",
        ],
        "expert": "Funkčné outdoorové textílie stoja na kombinácii vrstiev, povrchových úprav a konštrukcie odevu. Preto sa pri softshelle nedá spoľahnúť iba na názov materiálu. Starostlivosť má odstrániť pot, soľ a prach bez toho, aby na povrchu ostal film. Pri membránach a vodoodpudivých úpravách je dôležitý najmä správny prací prostriedok, dávka a oplach.",
        "sources": [
            ("GORE-TEX: Outerwear care", "https://www.gore-tex.com/support/care/outerwear"),
            ("Textile Exchange: Other synthetics", "https://textileexchange.org/other-synthetics/"),
        ],
        "related": [
            ("Ako prať softshell bundu a nohavice bez poškodenia membrány", "/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany"),
            ("Ako obnoviť impregnáciu softshellu po praní a kedy ju neriešiť", "/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit"),
            ("Kedy nepoužívať aviváž: uteráky, športové oblečenie, softshell aj detská bielizeň", "/n/kedy-nepouzivat-avivaz-uteraky-sportove-oblecenie-softshell-aj-detska-bielizen"),
        ],
        "faq": [
            ("Je softshell nepremokavý?", "Záleží od typu. Niektorý softshell má membránu, iný je hlavne vetruodolný a vodoodpudivý. Dôležitý je štítok a popis výrobcu."),
            ("Ako prať softshell s membránou?", "Šetrne, bez aviváže, s malou dávkou gélu a dobrým oplachom. Pri neistote použite postup odporúčaný výrobcom."),
            ("Kedy obnoviť impregnáciu softshellu?", "Až keď je čistý a suchý softshell, no voda sa už na povrchu neperlí."),
        ],
    },
    {
        "title": "Čo je membránové oblečenie: vodný stĺpec, priedušnosť a pranie bez poškodenia",
        "short": "Membránové oblečenie má chrániť pred dažďom a vetrom, ale zároveň odvádzať časť vlhkosti od tela. Pri praní je kľúčové neupchať povrch zvyškami gélu, aviváže alebo špiny.",
        "keywords": "čo je membránové oblečenie, vodný stĺpec, priedušnosť bundy, ako prať membránovú bundu, pranie membrány, funkčné oblečenie starostlivosť",
        "quick_title": "Rýchla odpoveď pre membránové bundy",
        "quick": [
            "<strong>Membrána je tenká funkčná vrstva.</strong> Má pomáhať s ochranou pred vodou a vetrom, no zároveň potrebuje čistý povrch a správnu starostlivosť.",
            "<strong>Vodný stĺpec nehovorí všetko.</strong> Dôležité sú aj švy, strih, vetranie, povrchová úprava a stav oblečenia po používaní.",
            "<strong>Aviváž vynechajte.</strong> Pri membránach môže zhoršiť priedušnosť, vodoodpudivosť a pocit čistoty.",
            "<strong>Perte menej často, ale správne.</strong> Pot, soľ a špina membráne neprospievajú, no agresívne pranie tiež nie.",
        ],
        "intro": [
            "Membránové oblečenie riešia turisti, lyžiari, rodičia detí v škôlke aj ľudia, ktorí chcú bundu do dažďa na bežné nosenie. Najčastejšie otázky sú: čo znamená vodný stĺpec, čo je priedušnosť, ako prať membránovú bundu a prečo bunda po čase premoká.",
            "Membrána je funkčná vrstva v textílii. Sama o sebe však nestačí. Výsledok ovplyvňujú aj vonkajšia látka, podlepené švy, zipsy, vetranie, kapucňa, vodoodpudivá úprava a stav oblečenia. Keď je bunda zanesená potom, soľou, prachom alebo zvyškami prostriedku, môže pôsobiť horšie aj bez toho, aby bola membrána zničená.",
            "Pri praní membránového oblečenia je preto cieľom odstrániť nečistoty a nezanechať film. Silná vôňa, aviváž alebo veľa gélu nie sú riešenie. Dôležitejšie je primerané dávkovanie, extra oplach a sušenie podľa štítku.",
        ],
        "property_rows": [
            ("Vodný stĺpec", "laboratórny údaj o odolnosti voči vode", "nezachráni znečistený povrch alebo poškodené švy"),
            ("Priedušnosť", "schopnosť odvádzať časť vlhkosti", "zhorší ju špina, film a nevhodná aviváž"),
            ("Švy a zipsy", "praktické slabé miesta oblečenia", "prať zapnuté a kontrolovať opotrebovanie"),
            ("Povrchová úprava", "pomáha vode perliť sa na povrchu", "obnovovať až na čistom suchom textile"),
        ],
        "care_rows": [
            ("Membránová bunda", "Zapnúť zipsy, prať podľa štítku, bez aviváže a s dobrým oplachom.", "Chráni membránu aj povrchovú úpravu."),
            ("Lyžiarske rukavice s membránou", "Čistiť podľa výrobcu, často radšej lokálne a šetrne.", "Vnútro a membrána schnú pomalšie."),
            ("Nepremokavé nohavice", "Najprv odstrániť blato, potom jemné pranie pri potrebe.", "Blato a piesok zvyšujú mechanické trenie."),
        ],
        "mistakes": [
            "Prať membránovú bundu s avivážou, aby bola mäkšia.",
            "Myslieť si, že vodný stĺpec vyrieši špinavé švy alebo poškodený zips.",
            "Sušiť membránu priamo na radiátore bez kontroly štítku.",
            "Impregnovať oblečenie bez predchádzajúceho vyčistenia.",
        ],
        "expert": "Pri membránovom oblečení sa často miešajú tri veci: nepremokavosť, vodoodpudivosť povrchu a priedušnosť. Každá funguje trochu inak. Pranie nemá membránu zázračne obnoviť, ale odstrániť pot, soľ a špinu tak, aby funkčné vrstvy mohli pracovať čo najlepšie. Preto výrobcovia technických materiálov zdôrazňujú starostlivosť podľa štítku a vynechanie nevhodných zmäkčujúcich prípravkov.",
        "sources": [
            ("GORE-TEX: Outerwear care", "https://www.gore-tex.com/support/care/outerwear"),
            ("Textile Exchange: Other synthetics", "https://textileexchange.org/other-synthetics/"),
        ],
        "related": [
            ("Ako prať softshell bundu a nohavice bez poškodenia membrány", "/n/ako-prat-softshell-bundu-a-nohavice-bez-poskodenia-membrany"),
            ("Ako odstrániť soľ a mokrý sneh z lyžiarskych rukavíc s membránou", "/n/ako-odstranit-sol-a-mokry-sneh-z-lyziarskych-rukavic-s-membranou"),
            ("Ako obnoviť impregnáciu softshellu po praní a kedy ju neriešiť", "/n/ako-obnovit-impregnaciu-softshellu-po-prani-a-kedy-ju-neriesit"),
        ],
        "faq": [
            ("Čo znamená vodný stĺpec?", "Je to údaj, ktorý popisuje odolnosť materiálu voči tlaku vody v laboratórnych podmienkach. Pri nosení však rozhodujú aj švy, strih a stav povrchu."),
            ("Ako prať membránovú bundu?", "Podľa štítku, bez aviváže, s primeranou dávkou pracieho prostriedku a dôkladným oplachom."),
            ("Prečo membránová bunda po praní premoká?", "Môže ísť o zvyšky prostriedku, oslabenú povrchovú úpravu, špinavý povrch alebo poškodenie. Najprv ju správne vyčistite a až potom riešte impregnáciu."),
        ],
    },
    {
        "title": "Prečo sa oblečenie žmolkuje: vlákna, trenie, pranie a sušenie",
        "short": "Žmolky vznikajú, keď sa voľné alebo poškodené vlákna na povrchu textilu zachytávajú do malých uzlíkov. Najčastejšie za tým je trenie pri nosení, pranie s drsnými kusmi a zlé sušenie.",
        "keywords": "prečo sa oblečenie žmolkuje, žmolky na svetri, ako zabrániť žmolkom, žmolkovanie pri praní, žmolky na tričku, odžmolkovač oblečenia",
        "quick_title": "Rýchla odpoveď proti žmolkom",
        "quick": [
            "<strong>Žmolky sú najmä problém povrchu látky.</strong> Vznikajú pri trení, keď sa vlákna uvoľnia a zamotajú.",
            "<strong>Najviac trpia miesta s pohybom.</strong> Pod pazuchami, na bokoch od kabelky, medzi stehnami, na rukávoch a pod kabátom.",
            "<strong>Pranie môže žmolky zhoršiť.</strong> Najmä ak miešate jemné úplety s uterákmi, zipsami alebo suchým zipsom.",
            "<strong>Pomáha triedenie podľa povrchu, nie iba podľa farby.</strong> Jemné pleteniny perte naruby a oddelene.",
        ],
        "intro": [
            "Žmolkovanie oblečenia je frustrujúce, pretože textil môže vyzerať staro už po niekoľkých noseniach. Najčastejšie sa riešia žmolky na svetri, žmolky na tričku, žmolky na kabáte, fleece mikine alebo teplákoch. Nie vždy to znamená, že je materiál nekvalitný, ale vždy to znamená, že povrch prešiel trením.",
            "Žmolky vznikajú tak, že sa vlákna na povrchu látky uvoľnia, zachytia sa o seba a vytvoria malé uzlíky. Niektoré materiály a zmesi sú na to náchylnejšie. Riziko zvyšuje voľnejší úplet, česaný povrch, krátke vlákna, trenie kabelky, batohu, kabáta, bezpečnostného pásu alebo prania s drsnými vecami.",
            "Ak chcete znížiť žmolkovanie pri praní, nestačí triediť iba biele a farebné. Triediť treba aj podľa povrchu: jemné úplety k jemným úpletom, uteráky zvlášť, zipsy zapnúť a suchý zips oddeliť. Pri žmolkoch je mechanika často dôležitejšia než prací prostriedok.",
        ],
        "property_rows": [
            ("Trenie pri nosení", "najmä rukávy, boky, pazuchy, stehná", "žmolky vznikajú aj bez prania"),
            ("Trenie v práčke", "zipsy, uteráky, suché zipsy a preplnený bubon", "jemné kusy prať naruby a oddelene"),
            ("Typ vlákna", "niektoré syntetiky a zmesi držia žmolky dlhšie", "sledovať zloženie aj povrch látky"),
            ("Sušenie", "presušenie a teplo môžu zhoršiť povrch", "sušiť podľa štítku, nie agresívne"),
        ],
        "care_rows": [
            ("Sveter", "Prať naruby, jemný program, nízke otáčky a sušenie naležato podľa štítku.", "Chráni tvar a povrch úpletu."),
            ("Fleece", "Oddeliť od drsných kusov, zapnúť zipsy, nepreplniť bubon.", "Česaný povrch reaguje na trenie."),
            ("Tričko so zmesou", "Prať s podobne hladkými kusmi a nepoužívať zbytočne horúce sušenie.", "Elastan a syntetika môžu meniť povrch."),
        ],
        "mistakes": [
            "Prať jemný sveter spolu s uterákmi alebo rifľami.",
            "Odstraňovať žmolky nožnicami tak, že prestrihnete vlákna.",
            "Myslieť si, že drahý prací gél sám zastaví trenie.",
            "Preplniť práčku, takže sa textil trie viac a horšie sa oplachuje.",
        ],
        "expert": "Žmolkovanie je výsledok mechanického namáhania povrchu textilu. Preto sa dá znížiť hlavne tým, že obmedzíte trenie pri nosení a praní. Prací prostriedok má textil vyčistiť, ale nemôže zmeniť konštrukciu látky. Pri materiálových zmesiach rozhoduje aj to, či sa uvoľnené vlákna odtrhnú, alebo ostanú držať v uzlíku na povrchu.",
        "sources": [
            ("Textile Exchange: Other synthetics", "https://textileexchange.org/other-synthetics/"),
            ("Materials market context", "https://textileexchange.org/knowledge-center/reports/materials-market-report-2024/"),
        ],
        "related": [
            ("Čo je polyester a ako ho prať, aby nezapáchal", "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"),
            ("Ako prať kašmírový sveter doma bez zrazenia a žmolkov", "/n/ako-prat-kasmirovy-sveter-doma-bez-zrazenia-a-zmolkov"),
            ("Akryl vs vlna: žmolkovanie, teplo, zápach a starostlivosť", "/n/akryl-vs-vlna-zmolkovanie-teplo-zapach-a-starostlivost"),
        ],
        "faq": [
            ("Ako zabrániť žmolkom na oblečení?", "Znížte trenie: perte naruby, nepreplňte bubon, jemné kúsky oddeľte od zipsov, uterákov a suchého zipsu."),
            ("Sú žmolky znakom nekvalitného materiálu?", "Niekedy áno, ale nie vždy. Žmolkovanie ovplyvňuje aj nosenie, kabelka, batoh, trenie pod kabátom a prací postup."),
            ("Ako odstrániť žmolky zo svetra?", "Použite odžmolkovač alebo hrebeň určený na textil a pracujte jemne. Pri vlne, kašmíre a jemných úpletoch postupujte veľmi opatrne."),
        ],
    },
    {
        "title": "Mikroplasty z oblečenia: ako prať syntetiku zodpovednejšie bez paniky",
        "short": "Syntetické textílie môžu pri praní uvoľňovať drobné vlákna. Riešením nie je panika, ale menej zbytočného prania, plnší no nie preplnený bubon, šetrnejší program a dlhšia životnosť oblečenia.",
        "keywords": "mikroplasty z oblečenia, mikrovlákna pri praní, syntetika a mikroplasty, ako prať syntetiku ekologickejšie, polyester mikroplasty, pranie funkčného oblečenia",
        "quick_title": "Rýchla odpoveď bez paniky",
        "quick": [
            "<strong>Mikroplasty z oblečenia sú najmä drobné syntetické vlákna.</strong> Môžu sa uvoľňovať pri praní, nosení aj sušení.",
            "<strong>Najpraktickejšie je predĺžiť životnosť oblečenia.</strong> Menej zbytočného prania, menej trenia a správne sušenie pomáha aj textilu.",
            "<strong>Práčku nepreplňte, ale neperte ani jeden kus samostatne.</strong> Primerane plný bubon znižuje zbytočný pohyb a šetrí zdroje.",
            "<strong>Silnejší program nie je automaticky lepší.</strong> Pri syntetike voľte podľa štítku a miery zašpinenia.",
        ],
        "intro": [
            "Mikroplasty z oblečenia sa často spájajú s polyesterom, polyamidom, akrylom, fleecem a funkčnou syntetikou. Presnejšie sa pri praní textilu často hovorí o mikrovláknach: drobných vláknach, ktoré sa môžu uvoľňovať z látky počas používania, prania a mechanického namáhania.",
            "Cieľom článku nie je strašiť. Syntetické materiály majú v domácnosti aj reálne výhody: rýchlo schnú, držia tvar, sú praktické pri športe a outdoor oblečení. Zodpovednejší prístup znamená prať ich rozumne, nepreťažovať textil zbytočným trením a kupovať menej vecí, ktoré sa rýchlo zničia.",
            "Ak hľadáte ako prať syntetiku ekologickejšie, začnite jednoduchými krokmi: vetrať namiesto zbytočného prania, prať až pri reálnej potrebe, dodržať primeranú náplň bubna, používať šetrnejší program a sušiť tak, aby oblečenie vydržalo čo najdlhšie.",
        ],
        "property_rows": [
            ("Zdroj", "syntetické textílie a ich povrchové opotrebovanie", "znížiť zbytočné trenie a pranie"),
            ("Pranie", "uvoľňovanie ovplyvňuje program, náplň a opotrebovanie", "neprať extrémne malé ani preplnené dávky"),
            ("Životnosť", "čím dlhšie vec slúži, tým menej často ju nahrádzate", "prať podľa štítku a opravovať drobné poškodenia"),
            ("Pach", "syntetiku treba prať včas po spotení", "neprekrývať pot vôňou, najprv riešiť čistotu"),
        ],
        "care_rows": [
            ("Športová syntetika", "Po spotení presušiť alebo vyprať v primeranej dávke.", "Vlhká taška zhoršuje pach aj potrebu agresívneho prania."),
            ("Fleece a česaná syntetika", "Prať naruby, oddelene od drsných kusov.", "Menej trenia znamená menej opotrebovania povrchu."),
            ("Outdoor oblečenie", "Prať podľa štítku a bez aviváže pri funkčných vrstvách.", "Funkčná úprava vydrží dlhšie pri šetrnej starostlivosti."),
        ],
        "mistakes": [
            "Prať jeden syntetický kus samostatne na intenzívnom programe bez potreby.",
            "Nechať spotené oblečenie zatuchnúť a potom používať agresívnejšie pranie.",
            "Používať aviváž na funkčné vrstvy, softshell alebo membrány.",
            "Kupovať nekvalitné syntetické veci, ktoré sa rýchlo žmolkujú a nahrádzajú.",
        ],
        "expert": "Výskum mikrovlákien ukazuje, že textílie môžu pri praní uvoľňovať drobné vlákna a že výsledok ovplyvňuje materiál, konštrukcia, opotrebovanie aj spôsob prania. Pre bežnú domácnosť je najrozumnejší prístup kombinovať dobrú starostlivosť, menej zbytočného prania a dlhšiu životnosť oblečenia. Nie je potrebné prestať používať syntetiku, ale dáva zmysel prať ju premyslene.",
        "sources": [
            ("Frontiers: Microplastics from textiles", "https://www.frontiersin.org/journals/marine-science/articles/10.3389/fmars.2019.00030/full"),
            ("Nature Scientific Reports: Microfiber release from textiles", "https://www.nature.com/articles/s41598-019-43023-x"),
            ("PMC: Microfibers and textiles context", "https://pmc.ncbi.nlm.nih.gov/articles/PMC9826067/"),
        ],
        "related": [
            ("Čo je polyester a ako ho prať, aby nezapáchal", "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"),
            ("Recyklovaný polyester: čo znamená, aké má výhody a ako sa oň starať", "/n/recyklovany-polyester-co-znamena-ake-ma-vyhody-a-ako-sa-on-starat"),
            ("Polyamid vs polyester: ktorý materiál lepšie znáša pot, šport a časté pranie", "/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie"),
        ],
        "faq": [
            ("Uvoľňuje polyester pri praní mikroplasty?", "Syntetické textílie môžu uvoľňovať drobné vlákna. Množstvo závisí od materiálu, konštrukcie, opotrebovania a prania."),
            ("Ako prať syntetiku zodpovednejšie?", "Neperte zbytočne, nepreplňte bubon, perte podľa štítku, znížte trenie a oblečenie dobre sušte."),
            ("Pomôže jemnejší program?", "Často áno, ak textil nie je silno zašpinený. Dôležité je aj správne triedenie a primeraná náplň práčky."),
        ],
    },
]


def main():
    articles = []
    times = ["08:00:00", "08:12:00", "08:24:00", "08:36:00", "08:48:00"]
    for index, article in enumerate(ARTICLES):
        long_html = helpers.build_long(article)
        if re.search(r"\bCTA\b", long_html):
            raise SystemExit(f"Forbidden customer-facing CTA wording in {article['title']}")
        if "Cena:" in long_html or re.search(r"\d+(?:[,.]\d{2})?\s*\u20ac", long_html):
            raise SystemExit(f"Fixed price wording in {article['title']}")
        if len(long_html) > 32700:
            raise SystemExit(f"XLS cell too long for {article['title']}: {len(long_html)}")
        articles.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": long_html,
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
        response = requests.get(url, timeout=30, allow_redirects=True, headers={"User-Agent": "Mozilla/5.0"})
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
                "slugs": [article["link"] for article in articles],
                "lengths": [len(article["long"]) for article in articles],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

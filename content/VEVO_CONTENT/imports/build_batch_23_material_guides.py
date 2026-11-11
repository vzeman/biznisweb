import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH_DATE = "2025-09-29"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-23-2026-06-11-articles.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-23-material-guides-clean-urls.xls"
HELPERS_PATH = Path("content/VEVO_CONTENT/imports/build_batch_21_material_guides.py")


spec = importlib.util.spec_from_file_location("batch21_helpers", HELPERS_PATH)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


ARTICLES = [
    {
        "title": "Čo je bambusová viskóza: mäkkosť, marketingové tvrdenia a reálna starostlivosť",
        "short": "Bambusová viskóza je regenerované celulózové vlákno z bambusového zdroja. Na dotyk býva mäkká, ale pri praní sa správa skôr ako viskóza než ako prírodný bambus.",
        "keywords": "čo je bambusová viskóza, bambusová viskóza pranie, bambusové oblečenie, bambusové vlákno marketing, ako prať bambusovú viskózu, bambusová viskóza starostlivosť",
        "quick_title": "Rýchla odpoveď bez marketingu",
        "quick": [
            "<strong>Bambusová viskóza nie je to isté ako surový bambus.</strong> Ide o regenerované celulózové vlákno, preto sa pri praní správajte opatrne ako pri jemnej viskóze.",
            "<strong>Mäkkosť je jej hlavná výhoda.</strong> Často ju nájdete v spodnej bielizni, pyžamách, tričkách, uterákoch alebo detskom textile.",
            "<strong>Pri praní rozhoduje hotový výrobok.</strong> Sledujte štítok, zmes, elastan, farbu a to, či ide o úplet alebo tkaninu.",
            "<strong>Marketingové tvrdenia neberte ako prací návod.</strong> Slovo bambus na obale neznamená, že textil znesie vysokú teplotu alebo sušičku.",
        ],
        "intro": [
            "Bambusová viskóza je obľúbená preto, že pôsobí mäkko, hladko a príjemne na pokožke. V obchodoch sa často spája s pojmami ako bambusové vlákno, bambusová bielizeň alebo bambusový textil. Pre zákazníka je však dôležité rozlíšiť marketingový názov od toho, ako sa materiál správa v práčke.",
            "Vo väčšine bežných textílií nejde o kus prírodného bambusu zapletený do látky. Bambus slúži ako zdroj celulózy, ktorá sa ďalej spracuje na regenerované vlákno podobné viskóze. Preto pri praní riešite najmä jemnosť, mokrý tvar, elastan a povrch látky.",
            "Ak sa pýtate, ako prať bambusovú viskózu, nezačínajte reklamou, ale štítkom. Inak sa bude správať bambusové tričko, inak spodná bielizeň s elastanom a inak uterák alebo osuška zo zmesi bavlny a bambusovej viskózy.",
        ],
        "property_rows": [
            ("Pocit", "mäkký, hladký, často príjemný na telo", "vhodná na bielizeň, pyžamá a tričká"),
            ("Mokrý tvar", "jemné úplety môžu byť citlivejšie", "nevešať ťažké mokré kusy za tenké miesta"),
            ("Savosť", "závisí od zmesi a konštrukcie", "uteráky a bielizeň úplne vysušiť"),
            ("Marketing", "slovo bambus môže byť zjednodušené označenie", "pracovať podľa štítku, nie podľa dojmu z názvu"),
        ],
        "care_rows": [
            ("Bambusové tričko", "Prať naruby, s podobnými farbami a bez preplnenia bubna.", "Chráni povrch, švy a tvar úpletu."),
            ("Bambusová spodná bielizeň", "Použiť vrecko na jemnú bielizeň a šetrný program.", "Elastan a gumičky nemajú rady teplo."),
            ("Bambusový uterák alebo osuška", "Nepreháňať aviváž, dobre opláchnuť a dosušiť.", "Zvyšky prípravkov môžu zhoršiť savosť aj sviežosť."),
        ],
        "mistakes": [
            "Považovať bambusovú viskózu za odolnejšiu len preto, že názov znie prírodne.",
            "Prať jemné bambusové kúsky spolu s ťažkými uterákmi a zipsami.",
            "Použiť priveľa pracieho prostriedku a krátky oplach.",
            "Sušiť horúco zmes s elastanom bez kontroly štítku.",
        ],
        "expert": "Pri bambusových textíliách je dôležité rozlišovať rastlinný zdroj celulózy od výsledného textilného vlákna. Spotrebiteľské autority upozorňujú, že veľa takzvaných bambusových látok je v skutočnosti rayon alebo viskóza vyrobená z bambusovej celulózy. Pri domácej starostlivosti preto dávajte prednosť štítku, zmesi a konštrukcii pred všeobecným marketingovým názvom.",
        "sources": [
            ("Textile Exchange: Manmade Cellulosic Fibers", "https://textileexchange.org/manmade-cellulosic-fibers/"),
            ("Britannica: Rayon textile fiber", "https://www.britannica.com/technology/rayon-textile-fiber"),
        ],
        "related": [
            ("Čo je viskóza: vlastnosti, krčivosť, zrážanie a starostlivosť", "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"),
            ("Modal vs lyocell vs viskóza: ako sa líšia pri praní a nosení", "/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni"),
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
        ],
        "faq": [
            ("Je bambusová viskóza prírodný materiál?", "Zdrojom celulózy môže byť bambus, ale výsledné vlákno je regenerované. Pri praní sa riaďte štítkom hotového výrobku."),
            ("Ako prať bambusové tričko?", "Naruby, šetrne, s podobnými farbami a bez preplnenia bubna. Sušte voľne podľa štítku."),
            ("Môže ísť bambusová viskóza do sušičky?", "Len ak to povoľuje výrobca. Pri jemných úpletoch a elastane je bezpečnejšie sušenie na vzduchu."),
        ],
    },
    {
        "title": "Bambusové vlákno vs bavlna: výhody, nevýhody a pranie pri citlivej pokožke",
        "short": "Bambusové textílie bývajú mäkké a príjemné, bavlna je univerzálna a známa. Pri citlivej pokožke však rozhoduje aj farbenie, zvyšky pracieho prostriedku a oplach.",
        "keywords": "bambusové vlákno vs bavlna, bambus alebo bavlna, bambusová bielizeň pranie, bavlna citlivá pokožka, bambusový uterák pranie, bambusová viskóza vs bavlna",
        "quick_title": "Rýchle porovnanie pre zákazníka",
        "quick": [
            "<strong>Bambusové textílie často vyhrávajú pocitom mäkkosti.</strong> Najmä pri bielizni a pyžamách môžu pôsobiť hladko a jemne.",
            "<strong>Bavlna je univerzálnejšia a ľahšie čitateľná.</strong> Viete ju nájsť v tričkách, uterákoch, posteľnej bielizni aj detskom oblečení.",
            "<strong>Pri citlivej pokožke nestačí riešiť vlákno.</strong> Dôležitý je prací prostriedok, dávka, oplach, farbivá a úplné vysušenie.",
            "<strong>Pri praní nerozhoduje len názov materiálu.</strong> Bambusová viskóza s elastanom potrebuje iný prístup než hrubý bavlnený uterák.",
        ],
        "intro": [
            "Porovnanie bambusové vlákno vs bavlna sa často objavuje pri spodnej bielizni, ponožkách, uterákoch, detských textíliách a pyžamách. Zákazník hľadá najmä mäkkosť, pohodlie a materiál vhodný na citlivú pokožku. V praxi však nejde o súboj jedného víťaza.",
            "Bavlna je prirodzené celulózové vlákno a v domácnosti sa používa veľmi široko. Bambusové textílie sú často regenerované celulózové vlákna z bambusového zdroja, teda materiálovo blízke viskóze. To znamená, že môžu byť veľmi príjemné, ale nie vždy znesú hrubé zaobchádzanie.",
            "Pri citlivej pokožke sa často zabúda na jednoduchú vec: podráždenie nemusí spôsobovať samotné vlákno. Môžu ho zhoršiť zvyšky pracieho gélu, aviváž, parfémovaná zložka, zle vysušený textil alebo farbivo. Preto má zmysel prať jemne, dávkovať rozumne a dôkladne oplachovať.",
        ],
        "property_rows": [
            ("Mäkkosť", "bambusová viskóza často veľmi jemná", "chrániť pred trením a vysokým teplom"),
            ("Univerzálnosť", "bavlna má široké použitie", "rozlišovať tričko, uterák, obliečky a bielizeň"),
            ("Citlivá pokožka", "závisí od celého výrobku", "jemná dávka gélu a dobrý oplach"),
            ("Savosť", "pri oboch závisí od väzby a zmesi", "neukladať vlhké textílie do skrine"),
        ],
        "care_rows": [
            ("Detské body", "Prať podľa štítku, jemne dávkovať a dôkladne opláchnuť.", "Pokožku často dráždia zvyšky v látke."),
            ("Spodná bielizeň", "Použiť vrecko na jemnú bielizeň a nižšie otáčky.", "Chráni gumičky, švy a elastan."),
            ("Uteráky", "Nezahltiť avivážou a dosušiť do sucha.", "Savosť aj sviežosť závisia od zvyškov prípravkov."),
        ],
        "mistakes": [
            "Vybrať materiál podľa jedného marketingového slova a ignorovať zloženie.",
            "Pri citlivej pokožke použiť veľa voňavého prípravku namiesto dôkladného oplachu.",
            "Prať jemnú bielizeň s uterákmi, zipsami alebo suchým zipsom.",
            "Uložiť mierne vlhké oblečenie do zásuvky a potom riešiť zatuchnutie.",
        ],
        "expert": "Bavlna aj bambusové regenerované vlákna vychádzajú z celulózy, ale vznikajú iným spôsobom a v látke sa správajú rozdielne. Pri citlivej pokožke je praktické hodnotiť celý výrobok: zloženie, farbivá, úpravy, švy, elastan a zvyšky pracieho prostriedku po praní.",
        "sources": [
            ("Textile Exchange: Manmade Cellulosic Fibers", "https://textileexchange.org/manmade-cellulosic-fibers/"),
            ("Britannica: Cotton", "https://www.britannica.com/topic/cotton-fibre-and-plant"),
        ],
        "related": [
            ("Čo je bavlna: vlastnosti, výhody, nevýhody a starostlivosť", "/n/co-je-bavlna-vlastnosti-vyhody-nevyhody-a-starostlivost"),
            ("Organická bavlna: čo znamená a či sa perie inak ako bežná bavlna", "/n/organicka-bavlna-co-znamena-a-ci-sa-perie-inak-ako-bezna-bavlna"),
            ("Ako prať detskú posteľnú bielizeň bez dráždenia pokožky", "/n/ako-prat-detsku-postelnu-bielizen-bez-drazdenia-pokozky"),
        ],
        "faq": [
            ("Je lepší bambus alebo bavlna?", "Záleží od použitia. Bambusová viskóza býva veľmi mäkká, bavlna je univerzálna a dobre známa."),
            ("Čo je lepšie pri citlivej pokožke?", "Sledujte celý výrobok a prací postup. Jemný materiál nepomôže, ak v látke ostanú zvyšky pracieho prostriedku."),
            ("Ako prať bambusové a bavlnené veci spolu?", "Len ak majú podobnú farbu, hmotnosť a štítok. Jemnú bielizeň radšej oddeľte od uterákov."),
        ],
    },
    {
        "title": "Čo je akryl: prečo pripomína vlnu a ako sa perie",
        "short": "Akryl je syntetické vlákno, ktoré sa často používa ako ľahšia a lacnejšia alternatíva vlny. Pri praní treba riešiť žmolkovanie, statiku, teplo a tvar úpletu.",
        "keywords": "čo je akryl, akryl v oblečení, akrylový sveter pranie, ako prať akryl, akryl žmolky, akryl vlastnosti",
        "quick_title": "Rýchla odpoveď pre úplety a svetre",
        "quick": [
            "<strong>Akryl nie je vlna, aj keď ju môže pripomínať.</strong> Je to syntetické vlákno používané v svetroch, šáloch, čiapkach a dekách.",
            "<strong>Najväčší problém bývajú žmolky a statika.</strong> Pomáha pranie naruby, menšie trenie a nepreplnený bubon.",
            "<strong>Teplo používajte opatrne.</strong> Horúca sušička alebo žehlenie môže zhoršiť tvar aj povrch.",
            "<strong>Pri zmesiach sledujte najcitlivejšiu zložku.</strong> Ak je v úplete vlna, elastan alebo iné vlákno, postup prispôsobte im.",
        ],
        "intro": [
            "Akryl sa v oblečení objavuje najmä tam, kde zákazník očakáva mäkkosť, hrejivosť a vzhľad podobný vlne, ale za nižšiu cenu alebo s jednoduchšou údržbou. Nájdete ho v svetroch, kardigánoch, čiapkach, šáloch, dekách, pončách a rôznych pletených doplnkoch.",
            "Pri praní akrylu veľa ľudí robí chybu, že ho berie ako odolnú syntetiku typu polyester. Akrylový úplet však môže strácať tvar, žmolkovať, elektrizovať a po zlom sušení pôsobiť lacno alebo vyťahane. Preto sa oplatí prať ho šetrne, naruby a bez zbytočného trenia.",
            "Akryl môže byť samostatný alebo v zmesi s vlnou, polyamidom, polyesterom či elastanom. Zloženie na štítku preto nie je formalita. Práve zmes rozhoduje, či môžete použiť bežný program, alebo radšej jemné pranie a sušenie naležato.",
        ],
        "property_rows": [
            ("Pocit", "môže pripomínať vlnu alebo mäkký úplet", "prať šetrne, aby povrch nežmolkoval"),
            ("Hrejivosť", "často sa používa v zimných doplnkoch", "po praní dobre presušiť bez prehriatia"),
            ("Statika", "môže elektrizovať", "nepresúšať horúco a nepreťažovať syntetické zmesi"),
            ("Tvar", "úplet sa môže vyťahať", "sušiť voľne alebo naležato podľa štítku"),
        ],
        "care_rows": [
            ("Akrylový sveter", "Prať naruby, jemný program, nízke otáčky.", "Znižuje trenie a riziko žmolkov."),
            ("Čiapka a šál", "Prať v sieťke alebo ručne podľa štítku.", "Malé úplety sa ľahko deformujú."),
            ("Akrylová deka", "Nepreplniť bubon a sušiť vzdušne.", "Veľký objem potrebuje priestor na oplach."),
        ],
        "mistakes": [
            "Sušiť akrylový úplet horúco a potom riešiť zmenený tvar.",
            "Prať sveter s drsnými zipsami, háčikmi alebo uterákmi.",
            "Odstraňovať žmolky agresívne tak, že poškodíte povrch.",
            "Ignorovať vlnu v zmesi len preto, že väčšina materiálu je akryl.",
        ],
        "expert": "Akrylové vlákna patria medzi syntetické textilné vlákna a často sa používajú pre vzhľad podobný vlne. V domácnosti je najdôležitejšie chrániť povrch úpletu pred trením a teplom. Žmolkovanie nevzniká iba z prania, ale aj z nosenia, trenia rukávov, kabelky, kabáta alebo sedenia.",
        "sources": [
            ("Britannica: Acrylic fiber", "https://www.britannica.com/technology/acrylic-fiber"),
            ("Britannica: Textile", "https://www.britannica.com/topic/textile"),
        ],
        "related": [
            ("Čo je polyester a ako ho prať, aby nezapáchal", "/n/co-je-polyester-a-ako-ho-prat-aby-nezapachal"),
            ("Čo je merino vlna: výhody, nevýhody a pranie bez zrazenia", "/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia"),
            ("Ako prať vlnený sveter, keď zapácha po nosení", "/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni"),
        ],
        "faq": [
            ("Je akryl prírodný materiál?", "Nie. Akryl je syntetické vlákno, ktoré môže vzhľadom a pocitom pripomínať vlnu."),
            ("Ako prať akrylový sveter?", "Naruby, šetrne, s nízkymi otáčkami a sušením podľa štítku. Pri zmesi s vlnou postupujte ešte opatrnejšie."),
            ("Prečo akryl žmolkuje?", "Najčastejšie kvôli treniu pri nosení a praní. Pomáha menšie mechanické namáhanie a pranie naruby."),
        ],
    },
    {
        "title": "Akryl vs vlna: žmolkovanie, teplo, zápach a starostlivosť",
        "short": "Akryl je syntetické vlákno, vlna je prírodné živočíšne vlákno. Oba materiály môžu hriať, ale líšia sa zápachom, žmolkovaním, tvarom a praním.",
        "keywords": "akryl vs vlna, akrylový sveter alebo vlnený, akryl žmolky, vlna pranie, akryl zápach, akryl a vlna rozdiel",
        "quick_title": "Rýchle rozhodnutie pri svetroch",
        "quick": [
            "<strong>Vlna lepšie pracuje s pachom a vlhkosťou.</strong> Vyžaduje však jemnejšiu starostlivosť a rešpekt k štítku.",
            "<strong>Akryl býva ľahší na cenu a dostupnosť.</strong> Častejšie však riešite statiku, žmolky a povrch po nosení.",
            "<strong>Pri oboch materiáloch škodí trenie.</strong> Sveter perte naruby, oddelene od zipsov a ťažkých textílií.",
            "<strong>Zmes akryl + vlna perte podľa citlivejšej zložky.</strong> Ak je na štítku vlna, neberte kus ako bežnú syntetiku.",
        ],
        "intro": [
            "Akryl vs vlna je praktická otázka pri svetroch, šáloch, čiapkach, dekách a zimných doplnkoch. Na prvý pohľad môžu vyzerať podobne: mäkký úplet, hrejivosť, zimný charakter. V práčke sa však správajú odlišne.",
            "Vlna je prírodné živočíšne vlákno, ktoré vie dobre pracovať s vlhkosťou a pachom, ale môže sa zraziť alebo splstnatieť pri zlom praní. Akryl je syntetika, ktorá sa často používa ako dostupnejšia alternatíva, ale môže elektrizovať, žmolkovať a horšie znášať teplo.",
            "Pri nákupe aj praní sa preto neoplatí hodnotiť len mäkkosť v ruke. Pozrite sa na zloženie, gramáž, spôsob pletenia a to, či je sveter určený na ručné pranie, program vlna alebo bežné jemné pranie.",
        ],
        "property_rows": [
            ("Pôvod", "akryl syntetický, vlna prírodná", "zmes prať podľa citlivejšej zložky"),
            ("Zápach", "vlna často potrebuje menej časté pranie", "najprv vetrať, až potom prať podľa štítku"),
            ("Žmolky", "akryl často výraznejšie pri trení", "prať naruby a nepreplniť bubon"),
            ("Teplo", "oba môžu hriať", "sušenie riešiť šetrne, nie horúco"),
        ],
        "care_rows": [
            ("Akrylový sveter", "Jemný program, naruby, nízke otáčky.", "Chráni povrch pred žmolkami."),
            ("Vlnený sveter", "Program vlna alebo ručne podľa štítku, vhodný prípravok.", "Vlna nemá rada teplotný šok a trenie."),
            ("Zmesový úplet", "Riadiť sa najcitlivejšou zložkou.", "Malý podiel vlny môže zmeniť celý prací postup."),
        ],
        "mistakes": [
            "Prať vlnený sveter ako akryl len preto, že je v zmesi.",
            "Odstraňovať žmolky hrubo a vytiahnuť vlákna z úpletu.",
            "Sušiť sveter zavesený tak, že sa vytiahne vlastnou váhou.",
            "Použiť silnú vôňu namiesto vetrania a správneho prania.",
        ],
        "expert": "Rozdiel medzi akrylom a vlnou nie je len v pocite. Vlna má inú štruktúru vlákna a iné správanie pri vlhkosti, teple a trení. Akryl ako syntetické vlákno môže napodobňovať niektoré vizuálne vlastnosti vlny, ale pri nosení a praní sa prejaví inak, najmä žmolkovaním a statickou elektrinou.",
        "sources": [
            ("Britannica: Acrylic fiber", "https://www.britannica.com/technology/acrylic-fiber"),
            ("Britannica: Wool", "https://www.britannica.com/animal/wool"),
        ],
        "related": [
            ("Čo je merino vlna: výhody, nevýhody a pranie bez zrazenia", "/n/co-je-merino-vlna-vyhody-nevyhody-a-pranie-bez-zrazenia"),
            ("Ako prať kašmírový sveter doma bez zrazenia a žmolkov", "/n/ako-prat-kasmirovy-sveter-doma-bez-zrazenia-a-zmolkov"),
            ("Ako prať vlnený sveter, keď zapácha po nosení", "/n/ako-prat-vlneny-sveter-ked-zapacha-po-noseni"),
        ],
        "faq": [
            ("Je lepší akryl alebo vlna?", "Záleží od rozpočtu a použitia. Vlna lepšie pracuje s pachom, akryl býva dostupnejší, ale môže viac žmolkovať."),
            ("Môže sa akryl zraziť?", "Zvyčajne nie ako vlna, ale teplo a zlé sušenie môžu zhoršiť tvar úpletu."),
            ("Ako prať zmes akrylu a vlny?", "Podľa štítku a citlivejšej zložky. Ak je v zmesi vlna, použite šetrný režim vhodný pre vlnu."),
        ],
    },
    {
        "title": "Čo je zmesový materiál: prečo sa oblečenie zráža alebo správa inak než čakáte",
        "short": "Zmesový materiál spája dve alebo viac vlákien. Preto sa tričko, rifle, šaty alebo sveter nemusia pri praní správať podľa jedného názvu na štítku.",
        "keywords": "čo je zmesový materiál, zmesové materiály pranie, bavlna polyester elastan, prečo sa oblečenie zráža, materiálové zloženie, ako prať zmesové oblečenie",
        "quick_title": "Rýchla odpoveď pri štítku",
        "quick": [
            "<strong>Zmesový materiál nemá jeden jednoduchý prací postup.</strong> Vždy sledujte všetky vlákna v zložení a pokyny výrobcu.",
            "<strong>Najcitlivejšia zložka často rozhoduje.</strong> Elastan, vlna, viskóza alebo membrána môžu obmedziť teplotu aj sušičku.",
            "<strong>Zrážanie nemusí spôsobovať iba bavlna.</strong> Tvar mení aj úplet, gramáž, švy, elastan a spôsob sušenia.",
            "<strong>Pri zmesiach je opatrnosť lacnejšia než oprava.</strong> Naruby, podobné farby, primeraná dávka gélu a nepreplnený bubon.",
        ],
        "intro": [
            "Zmesový materiál je veľmi bežný. Na štítku vidíte napríklad bavlna + elastan, polyester + bavlna, vlna + polyamid, viskóza + elastan alebo polyester + polyamid. Výrobca tým zvyčajne chce dosiahnuť pružnosť, nižšiu krčivosť, pevnosť, nižšiu cenu, mäkkosť alebo lepšie držanie tvaru.",
            "Pre domácu práčku to znamená, že sa nemôžete riadiť iba prvým slovom. Tričko s 95 % bavlny a 5 % elastanu už nie je rovnaká situácia ako čistá bavlnená utierka. Vlnené ponožky s polyamidom nie sú to isté ako syntetické športové ponožky. Viskózové šaty s elastanom môžu byť príjemné, ale pri mokrom stave citlivejšie na tvar.",
            "Ak sa oblečenie po praní zrazí, vyťahá alebo začne zvláštne zapáchať, príčina často nie je jedna. Môže ísť o kombináciu vlákna, konštrukcie látky, teploty, žmýkania, zvyškov pracieho prostriedku a sušenia.",
        ],
        "property_rows": [
            ("Pružnosť", "často vďaka elastanu", "neprať horúco a nesušiť agresívne"),
            ("Pevnosť", "polyamid alebo polyester môžu spevniť zmes", "pozor na zipsy a trenie"),
            ("Pocit", "bavlna, viskóza alebo modal zjemňujú dotyk", "chrániť farbu a mokrý tvar"),
            ("Zrážanie", "ovplyvňuje vlákno aj konštrukcia", "riadiť sa štítkom, nie iba názvom materiálu"),
        ],
        "care_rows": [
            ("Bavlna + elastan", "Prať naruby, nepreháňať teplotu, sušiť opatrne.", "Elastan drží tvar, ale neznáša zbytočné teplo."),
            ("Viskóza + elastan", "Jemný program, nízke otáčky, sušiť upravené do tvaru.", "Mokrý materiál sa môže vyťahať."),
            ("Vlna + polyamid", "Použiť režim vhodný pre vlnu podľa štítku.", "Vlna je citlivejšia zložka zmesi."),
        ],
        "mistakes": [
            "Prať zmes podľa najodolnejšej zložky namiesto najcitlivejšej.",
            "Ignorovať malý podiel elastanu, ktorý drží tvar oblečenia.",
            "Sušiť zmesové šaty alebo sveter zavesené tak, že sa vytiahnu.",
            "Použiť univerzálny horúci program na všetky materiály s označením bavlna.",
        ],
        "expert": "Textilné zmesi vznikajú preto, aby výsledná látka mala vlastnosti, ktoré jedno vlákno samo nedá. To je výhoda pri nosení, ale výzva pri praní. Domáca starostlivosť by mala vychádzať z najcitlivejšej zložky, konštrukcie odevu a odporúčaní výrobcu, nie z jediného marketingového názvu.",
        "sources": [
            ("Britannica: Textile", "https://www.britannica.com/topic/textile"),
            ("Trends on the cellulose-based textiles", "https://pmc.ncbi.nlm.nih.gov/articles/PMC8044815/"),
        ],
        "related": [
            ("Čo je elastan: prečo je v legínach, spodnej bielizni a športovom oblečení", "/n/co-je-elastan-preco-je-v-leginach-spodnej-bielizni-a-sportovom-obleceni"),
            ("Polyamid vs polyester: ktorý materiál lepšie znáša pot, šport a časté pranie", "/n/polyamid-vs-polyester-ktory-material-lepsie-znasa-pot-sport-a-caste-pranie"),
            ("Modal vs lyocell vs viskóza: ako sa líšia pri praní a nosení", "/n/modal-vs-lyocell-vs-viskoza-ako-sa-lisia-pri-prani-a-noseni"),
        ],
        "faq": [
            ("Ako prať zmesové materiály?", "Podľa štítku a najcitlivejšej zložky. Pri neistote zvoľte šetrnejší program, nižšie otáčky a sušenie na vzduchu."),
            ("Prečo sa zmesové oblečenie zrazilo?", "Môže ísť o teplotu, úplet, sušenie, elastan, viskózu alebo kombináciu viacerých faktorov."),
            ("Je zmesový materiál horší ako čistý?", "Nie automaticky. Zmesi môžu zlepšiť pružnosť, pevnosť alebo pohodlie, ale vyžadujú správnu starostlivosť."),
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
        if "Cena:" in long_html or re.search(r"\d+,\d{2}\s*€", long_html):
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
        response = requests.get(url, timeout=30, allow_redirects=True)
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
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

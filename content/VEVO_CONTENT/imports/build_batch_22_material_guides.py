import importlib.util
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests
import xlwt


BASE = "https://www.vevo.sk"
BATCH_DATE = "2025-09-30"
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-22-2026-06-11-articles.json")
OUT_XLS = Path.home() / "AppData/Local/Temp/vevo-batch-22-material-guides-clean-urls.xls"
HELPERS_PATH = Path("content/VEVO_CONTENT/imports/build_batch_21_material_guides.py")


spec = importlib.util.spec_from_file_location("batch21_helpers", HELPERS_PATH)
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)


ARTICLES = [
    {
        "title": "Čo je bavlna: vlastnosti, výhody, nevýhody a starostlivosť",
        "short": "Bavlna je prírodné celulózové vlákno, ktoré je príjemné na pokožke, savé a univerzálne. Pri praní však rozhoduje farba, úprava látky, zmes a teplota.",
        "keywords": "čo je bavlna, bavlna vlastnosti, bavlna výhody nevýhody, ako prať bavlnu, bavlnené tričko pranie, bavlnené obliečky pranie",
        "quick_title": "Rýchly praktický záver",
        "quick": [
            "<strong>Bavlna je savá a príjemná, ale nie nezničiteľná.</strong> Vysoká teplota a preplnená práčka môžu zhoršiť tvar aj farbu.",
            "<strong>Biele uteráky, farebné tričká a obliečky nie sú rovnaká situácia.</strong> Pri bavlne vždy rozlišujte farbu, gramáž a účel textilu.",
            "<strong>Zrážanie často súvisí s teplotou, sušením a konštrukciou látky.</strong> Najviac riskujú nové kusy, úplety a zmesi.",
            "<strong>Vôňa patrí až na čistú bavlnu.</strong> Ak uterák alebo tričko zapácha, najprv riešte dávkovanie, oplach a sušenie.",
        ],
        "intro": [
            "Bavlna patrí medzi najbežnejšie textilné materiály v domácnosti. Nájdete ju v tričkách, spodnej bielizni, detskom oblečení, uterákoch, obliečkach, kuchynských utierkach aj dekoračných textíliách. Je obľúbená preto, že je príjemná na telo, dobre saje vlhkosť a pri správnej starostlivosti zvládne časté pranie.",
            "Práve univerzálnosť bavlny však zvádza k príliš jednoduchému pravidlu: hodiť všetko bavlnené do jedného programu. V praxi sa inak správa tenké farebné tričko, inak hrubý biely uterák a inak bavlnené obliečky so zipsom alebo potlačou. Pri praní preto neriešte len slovo bavlna na štítku, ale aj konštrukciu výrobku.",
            "Bavlna môže byť tkaná, pletená, česaná, mercerovaná, zmesová alebo upravená farbivami a potlačou. Každá z týchto vecí mení to, ako textil znáša teplo, žmýkanie, sušičku a časté nosenie.",
        ],
        "property_rows": [
            ("Savosť", "dobre prijíma vlhkosť", "uteráky a obliečky treba úplne vysušiť"),
            ("Pocit na pokožke", "mäkký a prirodzený pri mnohých úpravách", "vhodná pre tričká, bielizeň a posteľ"),
            ("Zrážanie", "môže sa prejaviť pri teple a sušení", "nové kusy perte opatrnejšie podľa štítku"),
            ("Krčivosť", "mnohé bavlnené látky sa krčia", "pomáha správne vešanie a žehlenie podľa štítku"),
        ],
        "care_rows": [
            ("Bavlnené tričko", "Prať naruby, triediť podľa farby, nepreplniť bubon.", "Chráni farbu, potlač aj tvar úpletu."),
            ("Bavlnené obliečky", "Zapnúť zipsy, prať s podobnými farbami a dôkladne vysušiť.", "Vlhké skladovanie rýchlo zhoršuje sviežosť."),
            ("Bavlnené uteráky", "Nepreháňať aviváž, nechať doschnúť do sucha.", "Savosť sa zhoršuje zvyškami prípravkov a vlhkosťou."),
        ],
        "mistakes": [
            "Prať všetku bavlnu automaticky na vysokú teplotu bez ohľadu na farbu a potlač.",
            "Nechať hrubé bavlnené textílie dlho vlhké v práčke alebo v koši.",
            "Použiť priveľa gélu a potom sa čudovať tvrdosti alebo zatuchnutiu.",
            "Sušiť jemné bavlnené úplety horúco, keď štítok odporúča šetrnejší postup.",
        ],
        "expert": "Bavlna je celulózové prírodné vlákno s dobrou savosťou. To je výhoda pri uterákoch, posteľnej bielizni a každodennom oblečení, ale zároveň dôvod, prečo bavlna potrebuje dôkladné sušenie. Zvyšky vlhkosti, pracieho prostriedku a kožného mazu sú častou príčinou zatuchnutého dojmu.",
        "sources": [
            ("Britannica: Cotton", "https://www.britannica.com/topic/cotton-fibre-and-plant"),
            ("Textile Exchange: Organic Cotton Certification", "https://textileexchange.org/organic-cotton-certification/"),
        ],
        "related": [
            ("Polyester vs bavlna: rozdiely pri nosení, praní a vôni", "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
            ("Ako často prať posteľné prádlo", "/n/ako-casto-prat-postelne-pradlo"),
        ],
        "faq": [
            ("Na koľko stupňov prať bavlnu?", "Vždy podľa štítku. Biele uteráky znesú iný režim než farebné tričko s potlačou alebo elastanom."),
            ("Prečo sa bavlna zrazí?", "Najčastejšie kombináciou tepla, sušenia, konštrukcie látky a toho, či bol textil predzrazený."),
            ("Je bavlna vhodná na citlivú pokožku?", "Často áno, ale rozhoduje aj farbivo, povrchová úprava a použitý prací prostriedok."),
        ],
    },
    {
        "title": "Organická bavlna: čo znamená a či sa perie inak ako bežná bavlna",
        "short": "Organická bavlna označuje spôsob pestovania a certifikácie, nie automaticky iný domáci prací postup. Perie sa podľa štítku, farby a konštrukcie textilu.",
        "keywords": "organická bavlna, bio bavlna, organická bavlna pranie, ako prať organickú bavlnu, organická bavlna vs bavlna, GOTS bavlna",
        "quick_title": "Rýchla odpoveď bez marketingu",
        "quick": [
            "<strong>Organická bavlna neznamená automaticky jemnejšie pranie.</strong> Stále ide o bavlnené vlákno a rozhoduje konkrétny výrobok.",
            "<strong>Rozdiel je najmä v pestovaní a certifikácii.</strong> Pri praní sledujte štítok, farbu, úplet, potlač a zmes.",
            "<strong>Detské body, tričko a obliečky perte inak.</strong> Nie podľa slova organická, ale podľa účelu a konštrukcie.",
            "<strong>Pri citlivej pokožke pomáha dôkladný oplach.</strong> Aj kvalitná bavlna môže dráždiť, ak v nej ostanú zvyšky gélu.",
        ],
        "intro": [
            "Organická bavlna sa v obchodoch často prezentuje ako šetrnejšia voľba. Pre zákazníka je však dôležité oddeliť dve veci: ako bola bavlna pestovaná a ako sa o hotový textil starať doma. Domáca práčka totiž nečíta certifikát, ale pracuje s vláknom, farbou, potlačou, zmesou a konštrukciou odevu.",
            "Ak máte tričko, pyžamo alebo detské body z organickej bavlny, neznamená to, že ho treba prať úplne inak než bežnú bavlnu. Znamená to skôr, že sa oplatí nepokaziť materiál zbytočne agresívnym postupom: príliš vysokou teplotou, preplneným bubnom, veľkou dávkou pracieho prostriedku alebo horúcou sušičkou.",
            "Pri organickej bavlne sa často rieši aj citlivá pokožka. Tam je dôležité nielen samotné vlákno, ale aj farbivá, úpravy, zvyšky pracieho prostriedku a to, ako dobre je textil vysušený pred uložením do skrine.",
        ],
        "property_rows": [
            ("Pôvod", "súvisí so spôsobom pestovania", "prací postup určuje hotový výrobok"),
            ("Pocit", "môže byť mäkký, ale závisí od úpravy", "neodvodzovať program len z marketingového označenia"),
            ("Citlivá pokožka", "ľudia ju často vyhľadávajú pre deti a bielizeň", "dôležitý je oplach a jemné dávkovanie"),
            ("Zrážanie", "stále možné ako pri bavlne", "pozor na teplo, sušičku a úplet"),
        ],
        "care_rows": [
            ("Detské body", "Prať podľa štítku, dôkladne opláchnuť, úplne vysušiť.", "Zvyšky gélu môžu dráždiť viac než samotný materiál."),
            ("Organické bavlnené tričko", "Prať naruby a s podobnými farbami.", "Chráni farbu, potlač a povrch."),
            ("Posteľná bielizeň", "Zapnúť zipsy a nepreplniť bubon.", "Lepšie pranie aj oplach pri väčšom objeme."),
        ],
        "mistakes": [
            "Myslieť si, že organická bavlna sa nemôže zraziť.",
            "Prať detské oblečenie s príliš veľkou dávkou pracieho prostriedku.",
            "Ignorovať potlač, elastan alebo farbu len preto, že materiál je organický.",
            "Uložiť ešte vlhké body alebo pyžamo do zásuvky.",
        ],
        "expert": "Certifikácie organickej bavlny riešia najmä pôvod a spracovanie v dodávateľskom reťazci. Domáca starostlivosť je praktickejšia téma: zákazník potrebuje chrániť farbu, tvar a pokožku. Preto sa pri praní organickej bavlny oplatí postupovať skôr šetrne a dôkladne oplachovať.",
        "sources": [
            ("Textile Exchange: Organic Cotton Certification", "https://textileexchange.org/organic-cotton-certification/"),
            ("Britannica: Cotton", "https://www.britannica.com/topic/cotton-fibre-and-plant"),
        ],
        "related": [
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
            ("Ako odstrániť vitamínový sirup z detského body a podbradníka", "/n/ako-odstranit-vitaminovy-sirup-z-detskeho-body-a-podbradnika"),
            ("Ako často prať posteľné prádlo", "/n/ako-casto-prat-postelne-pradlo"),
        ],
        "faq": [
            ("Perie sa organická bavlna inak?", "Nie automaticky. Riaďte sa štítkom, farbou, potlačou a zmesou materiálu."),
            ("Je organická bavlna vhodná pre bábätká?", "Môže byť dobrá voľba, ale pri citlivej pokožke je rovnako dôležitý jemný prací prostriedok a dôkladný oplach."),
            ("Môže ísť organická bavlna do sušičky?", "Len ak to povoľuje štítok. Pri úpletoch a detskom oblečení je bezpečnejšie šetrné sušenie."),
        ],
    },
    {
        "title": "Čo je ľan: prečo sa krčí, ako ho prať a ako ho zjemniť",
        "short": "Ľan je prírodné vlákno z rastliny ľan. Je pevný, priedušný a savý, ale prirodzene sa krčí a pri praní potrebuje dostatok priestoru.",
        "keywords": "čo je ľan, ľan vlastnosti, ako prať ľan, ľanové oblečenie pranie, prečo sa ľan krčí, ako zjemniť ľan",
        "quick_title": "Rýchly praktický záver",
        "quick": [
            "<strong>Ľan sa krčí prirodzene.</strong> Nie je to chyba prania, ale vlastnosť vlákna a väzby.",
            "<strong>Pri praní potrebuje priestor.</strong> Preplnený bubon zvyšuje krčenie a nerovnomerný oplach.",
            "<strong>Zjemnenie prichádza používaním.</strong> Ľan býva mäkší po viacerých praniach, ak ho nepresúšate a nepreťažujete.",
            "<strong>Žehlenie riešte podľa výsledku, ktorý chcete.</strong> Elegantný ľan žehlite mierne vlhký, ležérny ľan nechajte prirodzene pokrčený.",
        ],
        "intro": [
            "Ľan je tradičné prírodné vlákno známe pevnosťou, priedušnosťou a typickým chladivým pocitom. V domácnosti ho nájdete v letnom oblečení, košeliach, šatách, obrusoch, utierkach, závesoch a posteľnej bielizni. Ľudia ho milujú pre prirodzený vzhľad, no zároveň sa ho boja kvôli krčeniu.",
            "Krčenie ľanu nie je porucha. Je to typický prejav vlákna a látky. Problém nastáva vtedy, keď očakávate dokonale hladký vzhľad bez žehlenia, alebo keď ľan periete v preplnenom bubne s ťažkými uterákmi a necháte ho preschnúť na tvrdú dosku.",
            "Dobrý ľanový textil môže vydržať roky, ale potrebuje rešpekt: triedenie farieb, šetrné žmýkanie, dostatok miesta v práčke a sušenie, ktoré ho nenechá zbytočne polámať.",
        ],
        "property_rows": [
            ("Priedušnosť", "výborná najmä v letnom oblečení", "nepreťažovať vôňou ani avivážou"),
            ("Savosť", "dobre prijíma vlhkosť", "sušiť úplne pred uložením"),
            ("Krčivosť", "prirodzená vlastnosť", "sušiť upravené do tvaru, žehliť mierne vlhké"),
            ("Pevnosť", "ľanové vlákno je pevné", "hotový výrobok stále chrániť pred agresívnym trením"),
        ],
        "care_rows": [
            ("Ľanová košeľa", "Prať naruby, nízke otáčky, vešať upravenú do tvaru.", "Znižuje výrazné záhyby a chráni švy."),
            ("Ľanové šaty", "Nepreplniť bubon, sušiť na ramienku alebo naležato podľa štítku.", "Mokrý kus môže meniť tvar."),
            ("Ľanový obrus", "Predprať škvrny, prať s podobnými farbami.", "Škvrny po jedle sa ľahšie riešia pred sušením."),
        ],
        "mistakes": [
            "Prať ľan s ťažkými uterákmi a očakávať hladký výsledok.",
            "Nechať ľan úplne preschnúť a potom sa snažiť žehliť hlboké záhyby.",
            "Preplniť bubon, čím sa látka viac láme a horšie oplachuje.",
            "Použiť agresívny program na jemný ľanový úplet alebo zmes.",
        ],
        "expert": "Ľan pochádza z rastliny flax a ako textilné vlákno je známy pevnosťou a priedušnosťou. Jeho typická krčivosť súvisí s tým, ako sa vlákno a väzba správajú pri ohybe. Domáca starostlivosť preto nemá krčivosť úplne odstrániť, ale udržať ľan čistý, mäkký a tvarovo použiteľný.",
        "sources": [
            ("Britannica: Flax", "https://www.britannica.com/plant/flax"),
            ("Britannica: Linen", "https://www.britannica.com/topic/linen"),
        ],
        "related": [
            ("Ako vybrať prací gél podľa typu bielizne", "/n/ako-vybrat-praci-gel-podla-typu-bielizne"),
            ("Ako prať ľanovú košeľu, aby nezostala tvrdá a pokrčená", "/n/ako-prat-lanovu-koselu-aby-nezostala-tvrda-a-pokrcena"),
            ("Čo je viskóza: vlastnosti, krčivosť, zrážanie a starostlivosť", "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"),
        ],
        "faq": [
            ("Prečo sa ľan tak krčí?", "Krčenie je prirodzená vlastnosť ľanového vlákna a látky. Dá sa zmierniť správnym praním, sušením a žehlením."),
            ("Ako zjemniť ľan?", "Pomáha pravidelné používanie, šetrné pranie, nepresúšanie a dostatok priestoru v bubne."),
            ("Môže ísť ľan do sušičky?", "Len ak to povoľuje štítok. Pri oblečení je bezpečnejšie sušenie na vzduchu upravené do tvaru."),
        ],
    },
    {
        "title": "Ľan vs bavlna: rozdiely v savosti, krčivosti a starostlivosti",
        "short": "Ľan aj bavlna sú prírodné celulózové vlákna, ale líšia sa pocitom, krčivosťou, priedušnosťou a správaním pri praní.",
        "keywords": "ľan vs bavlna, ľan alebo bavlna, ľanové obliečky vs bavlnené, ľan krčivosť, bavlna savosť, prírodné materiály pranie",
        "quick_title": "Rýchle porovnanie",
        "quick": [
            "<strong>Bavlna je univerzálnejšia a známejšia.</strong> Hodí sa na tričká, uteráky, posteľ a každodenné pranie.",
            "<strong>Ľan je priedušný a pevný, ale krčivejší.</strong> Je výborný na leto, obrusy, košele a prirodzený vzhľad.",
            "<strong>Pri praní ľanu nerobte z neho bavlnu.</strong> Potrebuje viac priestoru a opatrnejšie sušenie.",
            "<strong>Pri bavlne rozlišujte typ textilu.</strong> Uterák, tričko a obliečky majú rozdielne nároky.",
        ],
        "intro": [
            "Porovnanie ľan vs bavlna riešia ľudia pri výbere oblečenia, posteľnej bielizne, utierok, obrusov aj letných kúskov. Oba materiály sú prírodné celulózové vlákna, ale v domácnosti sa správajú rozdielne. Bavlna pôsobí známejšie a univerzálnejšie, ľan má výraznejší charakter, vyššiu krčivosť a prirodzenejší vzhľad.",
            "Ak chcete hladké tričko bez žehlenia, ľan vás môže hnevať. Ak chcete vzdušnú košeľu alebo obrus s prirodzenou textúrou, práve ľan môže byť výhoda. Pri praní je najdôležitejšie nehádať sa s materiálom: bavlnu nepreťažovať zvyškami prípravkov a ľanu dať priestor.",
            "Rozdiel medzi ľanom a bavlnou sa naplno ukáže až po niekoľkých praniach. Bavlna môže mäknúť a zároveň strácať farbu, ľan môže pôsobiť príjemnejšie, ale stále si ponechá typickú krčivosť.",
        ],
        "property_rows": [
            ("Priedušnosť", "ľan často pôsobí vzdušnejšie", "pri letnom oblečení menej prevoňovať a dobre vetrať"),
            ("Savosť", "oba materiály sajú vlhkosť", "pred uložením musia byť úplne suché"),
            ("Krčivosť", "ľan výraznejšia, bavlna podľa väzby", "ľan sušiť upravený, bavlnu triediť podľa typu"),
            ("Údržba", "bavlna znáša širšie použitie", "ľan potrebuje viac priestoru a šetrnejšie žmýkanie"),
        ],
        "care_rows": [
            ("Letná košeľa", "Pri ľane nízke otáčky a vešanie do tvaru, pri bavlne podľa potlače a farby.", "Chráni vzhľad a znižuje krčenie."),
            ("Posteľná bielizeň", "Nepreplniť bubon a úplne vysušiť.", "Veľký objem potrebuje priestor na pranie aj oplach."),
            ("Kuchynské utierky", "Prať oddelene podľa znečistenia.", "Mastnota a kuchynské pachy sa ľahko prenášajú."),
        ],
        "mistakes": [
            "Očakávať od ľanu hladkosť polyesteru alebo jemnej bavlny.",
            "Prať ľanové a bavlnené kusy bez triedenia podľa farby a hmotnosti.",
            "Nechať posteľnú bielizeň vlhkú v koši alebo v práčke.",
            "Používať vôňu ako náhradu za dôkladné vypranie kuchynských textílií.",
        ],
        "expert": "Ľan aj bavlna patria medzi celulózové vlákna, no pochádzajú z iných rastlinných zdrojov a v látke sa správajú odlišne. Bavlna je v domácnosti univerzálnejšia, ľan má vyššiu prirodzenú textúru a krčivosť. Preto by sa porovnanie nemalo končiť otázkou, čo je lepšie, ale na čo materiál používate.",
        "sources": [
            ("Britannica: Cotton", "https://www.britannica.com/topic/cotton-fibre-and-plant"),
            ("Britannica: Linen", "https://www.britannica.com/topic/linen"),
        ],
        "related": [
            ("Polyester vs bavlna: rozdiely pri nosení, praní a vôni", "/n/polyester-vs-bavlna-rozdiely-pri-noseni-prani-a-voni"),
            ("Ako správne prať obliečky", "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"),
            ("Ako často prať posteľné prádlo", "/n/ako-casto-prat-postelne-pradlo"),
        ],
        "faq": [
            ("Je lepší ľan alebo bavlna?", "Záleží od použitia. Bavlna je univerzálna, ľan je vzdušný a prirodzene krčivý."),
            ("Čo sa menej krčí?", "Zvyčajne bavlna, ale záleží od väzby a úpravy. Ľan je krčivý prirodzene."),
            ("Ako prať ľanové obliečky?", "Nepreplniť bubon, prať podľa štítku, nižšie otáčky a dôkladne vysušiť pred uložením."),
        ],
    },
    {
        "title": "Modal vs lyocell vs viskóza: ako sa líšia pri praní a nosení",
        "short": "Modal, lyocell a viskóza patria medzi regenerované celulózové vlákna. Líšia sa pocitom, splývavosťou, citlivosťou na mokro a tým, ako ich treba prať.",
        "keywords": "modal vs lyocell vs viskóza, modal alebo lyocell, lyocell vs viskóza, modal pranie, viskóza pranie, regenerované celulózové vlákna",
        "quick_title": "Rýchle porovnanie pre domácnosť",
        "quick": [
            "<strong>Viskóza býva splývavá, ale môže byť citlivejšia na mokro.</strong> Pri praní ju nepreťažujte a sušte opatrne.",
            "<strong>Modal často pôsobí mäkko a príjemne na telo.</strong> Často ho nájdete v bielizni, pyžamách a tričkách.",
            "<strong>Lyocell býva hladký, jemný a chladivý.</strong> Pri praní chráňte povrch a tvar hotového výrobku.",
            "<strong>Pri všetkých troch rozhoduje zmes.</strong> Elastan, bavlna alebo polyester v zložení menia postup.",
        ],
        "intro": [
            "Modal, lyocell a viskóza sa často hádžu do jedného vreca, pretože všetky patria medzi regenerované celulózové vlákna. Pre zákazníka to znamená, že nie sú klasickou bavlnou, ale ani syntetikou typu polyester. V obchodoch ich vidíte na tričkách, šatách, blúzkach, pyžamách, spodnej bielizni a niekedy aj na posteľnej bielizni.",
            "Rozdiely medzi nimi sú praktické: viskóza môže byť krásne splývavá, ale pri mokrom stave citlivejšia; modal je často veľmi mäkký a príjemný na telo; lyocell pôsobí hladko, chladivo a komfortne. Pri praní však nikdy nestačí čítať iba názov vlákna. Rozhoduje aj úplet, tkanina, farba, elastan a pokyny výrobcu.",
            "Ak sa pýtate, či je lepší modal, lyocell alebo viskóza, najprv si odpovedzte, na čo textil používate. Na spodnú bielizeň oceníte mäkkosť, na šaty splývavosť, na posteľnú bielizeň príjemný pocit a dobré sušenie.",
        ],
        "property_rows": [
            ("Viskóza", "splývavá, príjemná, môže sa krčiť", "prať šetrne a chrániť mokrý tvar"),
            ("Modal", "mäkký a hladký pocit", "vhodný na bielizeň, pyžamá a tričká"),
            ("Lyocell", "jemný, hladký, často chladivý", "chrániť povrch a nepreplniť bubon"),
            ("Zmesi", "často s elastanom alebo bavlnou", "riadiť sa najcitlivejšou zložkou"),
        ],
        "care_rows": [
            ("Blúzka z viskózy", "Prať naruby, nízke otáčky, sušiť upravenú do tvaru.", "Mokrý materiál môže meniť tvar."),
            ("Modalová bielizeň", "Použiť vrecko na jemnú bielizeň a šetrný program.", "Chráni gumičky, švy a elastan."),
            ("Lyocellové tričko", "Nepreplniť bubon, primeraná dávka gélu, sušiť voľne.", "Povrch ostane hladší a menej namáhaný."),
        ],
        "mistakes": [
            "Prať viskózové šaty s ťažkými uterákmi.",
            "Sušiť elastické modalové kúsky horúco bez kontroly štítku.",
            "Použiť veľa pracieho prostriedku a krátky oplach.",
            "Vešať ťažký mokrý kus tak, že sa vytiahne vlastnou váhou.",
        ],
        "expert": "Regenerované celulózové vlákna vznikajú spracovaním celulózy do textilného vlákna. Pre bežné pranie je dôležitejší hotový výrobok než samotná chemická kategória. Jemný úplet, elastan a farbivá môžu byť citlivejšie než vlákno ako také.",
        "sources": [
            ("Britannica: Rayon textile fiber", "https://www.britannica.com/technology/rayon-textile-fiber"),
            ("Trends on the cellulose-based textiles", "https://pmc.ncbi.nlm.nih.gov/articles/PMC8044815/"),
        ],
        "related": [
            ("Čo je viskóza: vlastnosti, krčivosť, zrážanie a starostlivosť", "/n/co-je-viskoza-vlastnosti-krcivost-zrazanie-a-starostlivost"),
            ("Modal v oblečení: čo znamená, prečo je mäkký a ako ho prať", "/n/modal-v-obleceni-co-znamena-preco-je-maekky-a-ako-ho-prat"),
            ("Čo je lyocell alebo Tencel", "/n/co-je-lyocell-alebo-tencel-priedusnost-jemnost-a-starostlivost"),
        ],
        "faq": [
            ("Je modal lepší ako viskóza?", "Nie vždy. Modal často pôsobí mäkšie, ale rozhoduje konkrétna látka, zmes a účel oblečenia."),
            ("Je lyocell to isté ako viskóza?", "Patria do príbuznej skupiny regenerovaných celulózových vlákien, ale nejde o úplne rovnaký materiál."),
            ("Ako prať šaty z viskózy, modalu alebo lyocellu?", "Naruby, šetrne, bez preplnenia bubna a so sušením upraveným do tvaru podľa štítku."),
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

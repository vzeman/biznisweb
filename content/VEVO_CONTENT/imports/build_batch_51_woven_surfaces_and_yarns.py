#!/usr/bin/env python3
"""Build and validate VEVO batch 51 woven-surface articles."""

from __future__ import annotations

import json
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

from build_batch_50_textile_structures_and_fillings import (
    BASE,
    FIXED_PRICE_RE,
    FORBIDDEN_PUBLIC_RE,
    WORD_RE,
    article_hrefs,
    fetch_status,
    render_article,
    visible_text,
)


PUBLISH_DATE = "2026-08-27"
CANDIDATES = Path("content/VEVO_CONTENT/batches/batch-51-candidates-2026-08-27.txt")
OUT_JSON = Path("content/VEVO_CONTENT/imports/batch-51-2026-08-27-articles.json")
OUT_PREFLIGHT = Path("content/VEVO_CONTENT/exports/batch-51-2026-08-27-link-preflight.json")

EU_FIBRE_LABEL = "https://eur-lex.europa.eu/eli/reg/2011/1007/oj"
GINETEX = "https://www.ginetex.net/share/article/4201/care-symbols"
AATCC_DIMENSION = "https://members.aatcc.org/store/tm135/543/"
AATCC_COLOR = "https://members.aatcc.org/store/tm61/495/"
COTTONWORKS_WEAVING = "https://cottonworks.com/wp-content/uploads/2023/03/Weaving-101.pdf"
COTTONWORKS_BASIC_WEAVES = "https://cottonworks.com/learning-hub/weaving/basic-woven-fabric-designs/"
COTTONWORKS_QUALITY = "https://cottonworks.com/learning-hub/quality-assurance/quality-testing/"
COTTONWORKS_SHRINKAGE = "https://cottonworks.com/learning-hub/quality-assurance/shrinking-and-skewing/"
COTTONWORKS_DENIM = "https://cottonworks.com/learning-hub/denim/denim-basics/"
COTTONWORKS_CHENILLE = "https://cottonworks.com/encyclopedia-item/chenille/"
COTTONWORKS_YARNS = "https://cottonworks.com/wp-content/uploads/2017/11/Textile_Yarns.pdf"
ASTM_COUNT = "https://store.astm.org/d3775-17r23.html"
ASTM_TEAR = "https://store.astm.org/d1424-25.html"
ASTM_ABRASION = "https://store.astm.org/d4966-22r26.html"
ASTM_SNAG = "https://store.astm.org/d3939_d3939m-26.html"
ASTM_DISTORTION = "https://store.astm.org/d1336-07r25e01.html"
ASTM_SEAM = "https://store.astm.org/d1683_d1683m-17e01.html"
ASTM_UPHOLSTERY_SLIP = "https://store.astm.org/d4034_d4034m-26.html"
PMC_SURFACE_ABRASION = "https://pmc.ncbi.nlm.nih.gov/articles/PMC12348827/"
PMC_CHENILLE = "https://pmc.ncbi.nlm.nih.gov/articles/PMC11724904/"
MATELASSE_WEAVES = "https://www.arahne.si/tutorials/mattelase-fabric-with-a-shell/"
MARSEILLE_QUILTING = "https://www.internationalquiltmuseum.org/exhibition/marseille-white-corded-quilting"

ARTICLE_LABEL = "/n/ako-citat-stitok-na-obleceni-material-symboly-prania-a-spravny-program"
ARTICLE_STAIN = "/n/ako-odstranit-zuvacku-krv-vosk-a-ine-skvrny-z-oblecenia"
ARTICLE_COLOR = "/n/stalofarebnost-textilu-preco-farby-blednu-pri-prani-svetle-a-treni"
ARTICLE_SHRINKAGE = "/n/preco-sa-oblecenie-zrazi-po-prani-teplota-vlakna-susicka-a-prevencia"
ARTICLE_ODOR = "/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia"
ARTICLE_DRYING = "/n/ako-susit-bielizen-v-malom-byte-bez-zatuchnutia"
ARTICLE_IRONING = "/n/ako-vyzehlit-koselu-kompletny-sprievodca-pre-dokonaly-vysledok"
ARTICLE_PILLING = "/n/preco-sa-oblecenie-zmolkuje-vlakna-trenie-pranie-a-susenie"
ARTICLE_SNAGGING = "/n/zatrhavanie-textilu-preco-vznikaju-vytiahnute-ocka-a-ako-im-predchadzat"
ARTICLE_DENIM = "/n/bavlna-a-elastan-starostlivost-o-tricka-rifle-a-spodnu-bielizen"
ARTICLE_POPLIN = "/n/co-je-popelin-hladka-koselova-tkanina-vlastnosti-a-starostlivost"
ARTICLE_DAMASK = "/n/co-je-damask-obojstranny-tkany-vzor-a-starostlivost-o-obrusy-a-obliecky"
ARTICLE_BEDDING = "/n/ako-spravne-prat-obliecky-kompletny-sprievodca-starostlivostou"
ARTICLE_BLANKET = "/n/ako-prat-prehozy-na-gauc-a-dekoracne-prikryvky"
ARTICLE_BOUCLE = "/n/co-je-bukle-sluckovy-povrch-zmolky-a-setrne-cistenie"
ARTICLE_VELVET = "/n/co-je-velur-vlasovy-povrch-rozdiel-od-zamatu-a-starostlivost"
ARTICLE_CANVAS = "/n/co-je-canvas-pevne-platno-skvrny-a-spravne-pranie"
ARTICLE_PERCALE = "/n/co-je-perkal-husta-tkanina-na-obliecky-vlastnosti-a-pranie"

LAUNDRY_PRODUCT_NAME = "Prací gél hypoalergénny Vevo Ylang Absolute 1L"
LAUNDRY_PRODUCT_URL = "/p-1627/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l"
LAUNDRY_CATEGORY_NAME = "Pracie gély"
LAUNDRY_CATEGORY_URL = "/c/vevo-home-care/pranie/praci-gel"


def commercial(article: dict[str, object], *, noun: str, limit: str) -> None:
    article.update(
        {
            "product_heading": f"Prací gél použite iba na prateľný {noun}",
            "product_intro": (
                f"Ak etiketa celého výrobku povoľuje domáce pranie a zloženie je kompatibilné, "
                f"tekutý gél možno dávkovať podľa tvrdosti vody, veľkosti náplne a miery znečistenia."
            ),
            "product_name": LAUNDRY_PRODUCT_NAME,
            "product_url": LAUNDRY_PRODUCT_URL,
            "product_text": (
                "Tekutý prostriedok sa dá presne odmerať a pri primeranej náplni dobre opláchnuť. "
                "Nenalievajte koncentrát priamo na suchý reliéf, vlas ani farebnú lícnu plochu."
            ),
            "product_limit": limit,
            "category_heading": "Prací prostriedok vyberajte podľa vlákna a celého výrobku",
            "category_intro": (
                "Obchodný názov tkaniny alebo priadze neurčuje jednu vhodnú receptúru. Najprv "
                "skontrolujte percentá vlákien, farbu, podšívku, výstuž, povrch a symboly ošetrovania."
            ),
            "category_name": LAUNDRY_CATEGORY_NAME,
            "category_url": LAUNDRY_CATEGORY_URL,
            "category_text": (
                "V kategórii nájdete gély pre rôzne potreby bežnej prateľnej bielizne. Zvoľte "
                "kompatibilný výrobok, dodržte dávku a pri špeciálnom vlákne rešpektujte pokyn výrobcu."
            ),
        }
    )


CHAMBRAY: dict[str, object] = {
    "title": "Čo je chambray: farebná osnova, svetlý útok a správne pranie",
    "link": "co-je-chambray-farebna-osnova-svetly-utok-a-spravne-pranie",
    "meta": "Čo je chambray, ako sa líši od denimu a popelínu a ako prať, sušiť a žehliť chambray košeľu bez máp, blednutia a poškodenia goliera.",
    "short": "Chambray je ľahšia tkanina, zvyčajne v plátnovej väzbe, pri ktorej farebná osnova a svetlý útok vytvárajú jemne melírovaný vzhľad. Nie je to samostatné vlákno ani automaticky tenký denim.",
    "name": "chambray",
    "locative": "chambray",
    "identity_heading": "Chambray opisuje tkaninu a farebnú konštrukciu, nie jedno vlákno",
    "identity_detail": "Typický chambray spája jednoduchú plátnovú väzbu s rozdielne sfarbenou osnovou a útkom, takže z malej vzdialenosti pôsobí jednofarebne a zblízka ukáže miešanie nití.",
    "identity_boundary": "Najčastejší bavlnený variant môže vyzerať podobne ako zmes s polyesterom, ľanom alebo elastanom, no ich savosť, krčivosť, návratnosť a teplotný limit sa líšia.",
    "label_focus": "presné zloženie, elastan, podlep goliera a manžiet, nite, gombíky, potlač, výšivku, farbenie priadze, povrchovú úpravu a povolené žehlenie",
    "missing_label": "Pri metráži si vyžiadajte technický list a urobte skúšobné predpranie; pri hotovej košeli bez etikety nezačínajte automaticky programom pre bavlnu iba podľa modrého melíru.",
    "dry_check": "odreté hrany goliera a manžiet, svetlé zalomenia, mapy od potu, poškodené gombíky, rozostúpené nite pri šve, cudziu farbu, vytiahnutú priadzu a lesk po žehlení",
    "damage_boundary": "Mastný golier alebo zvyšok dezodorantu možno čistiť, no mechanicky vyšúchaná hrana, prasknutá priadza a trvalý lesk po vysokej teplote nie sú škvrny.",
    "test_focus": "Pri tmavom alebo indigom farbenom kuse sledujte po vyschnutí nielen odtieň, ale aj prenos farby na bielu handričku a rozdiel medzi lícom, rubom a švom.",
    "combined_risk": "uvoľnenia farbiva, napučania priadze, trenia na exponovaných hranách a rozdielneho zrazenia košeľoviny, šijacej nite a výstuže",
    "chemistry_boundary": "Pot, kožný maz, make-up a pigment z pera potrebujú odlišný prvý krok; silný odstraňovač na golieri môže vytvoriť svetlý oblúk, ktorý je viditeľnejší než pôvodná škvrna.",
    "drying_detail": "Košeľu otvorte pri manžetách, golieri a lége, aby vzduch dosiahol pod dvojité vrstvy; šaty podoprite v ramenách a metráž nerozložte cez ostrú špinavú hranu.",
    "heat_boundary": "Vysoká teplota môže zvýrazniť zrazenie bavlny, poškodiť elastan, zmeniť živicovú úpravu, vytvoriť lesk alebo odtlačiť švové rezervy na líc.",
    "stop_signs": "silný prenos farby, rastúca svetlá mapa, zvlnenie podlepenej výstuže, otváranie šva, lepkavý povrch, praskanie potlače alebo deformácia gombíka",
    "professional_boundary": "Bežnú prateľnú chambray košeľu možno ošetrovať doma podľa etikety, no sako, podšitý odev, historický indigový kus alebo výrobok určený na profesionálne čistenie potrebuje presnejšie posúdenie.",
    "answer": "Chambray je spravidla ľahká tkanina v plátnovej väzbe, často s modrou alebo inak farebnou osnovou a bielym či svetlým útkom. Preto pôsobí melírovane, ale na rozdiel od klasického denimu nemá typické diagonálne rebro keprovej väzby. Názov neurčuje zloženie ani program prania. Najprv prečítajte etiketu celej košele, skontrolujte golier, manžety, výstuž a stálofarebnosť, potom perte s podobnými farbami v primerane naplnenom bubne. Koncentrovaný prostriedok nelejte priamo na líc. Košeľu vyberte hneď po cykle, urovnajte švy, sušte v tieni a žehlite iba pri povolenej teplote z rubu alebo cez ochrannú tkaninu.",
    "intro": "Otázka ako prať chambray košeľu sa často zjednoduší na radu pre denim, pretože oba materiály môžu mať modrý a biely efekt. Konštrukčne však nejde o to isté. Chambray býva ľahší, plátnovo tkaný a používa sa na košele, blúzky, šaty, zástery aj ľahké bytové doplnky. O praní rozhoduje zloženie priadze, spôsob farbenia, výstuž goliera, potlač a spracovanie hotového výrobku. Dobrý postup preto rieši farbu, mastnotu na kontaktných zónach, rozmer, šev a žehlenie ako jeden systém.",
    "quick": [
        "<strong>Pozrite sa na väzbu:</strong> chambray má typicky jednoduché kríženie nití, denim diagonálnu keprovú líniu.",
        "<strong>Melír vzniká priadzami:</strong> svetlý a farebný smer sa opticky premiešajú, nejde iba o potlač na hotovej látke.",
        "<strong>Modrá môže púšťať:</strong> nový alebo indigom farbený kus perte podľa etikety s podobnými odtieňmi.",
        "<strong>Golier čistite lokálne a šetrne:</strong> mastnotu uvoľnite kompatibilným postupom bez tvrdého drhnutia farebnej hrany.",
        "<strong>Nepreplňujte bubon:</strong> košeľa potrebuje pohyb a oplach, inak ostanú ostré lomy a zvyšky pri švoch.",
        "<strong>Žehlenie skúšajte z rubu:</strong> elastan, potlač, výstuž alebo úprava môžu mať nižší limit než bavlnená priadza.",
    ],
    "overview_heading": "Ako chambray vzniká a prečo vyzerá inak zblízka",
    "overview": [
        "V tkanej látke vedie osnova pozdĺžne a útok sa vkladá priečne. Pri plátnovej väzbe sa každá niť pravidelne strieda nad a pod susednou sústavou. Ak je jedna sústava farebná a druhá svetlá, jednotlivé body sa pri bežnej pozorovacej vzdialenosti opticky spoja. Výsledkom je jemný krížový melír, ktorý zároveň ukáže každú zmenu povrchu a farby pod iným uhlom.",
        "CottonWorks uvádza plain-weave chambray ako konštrukciu odlišnú od bežného denimu, ktorý sa spája najmä s osnovným keperom. Toto rozlíšenie pomáha pri identifikácii, nie pri určovaní teploty. Chambray môže mať farbenú osnovu aj opačné usporiadanie, môže byť kusovo farbený alebo praný pre mäkší vzhľad a môže obsahovať rôzne vlákna. Presná etiketa má preto prednosť pred typickým opisom.",
        "Na košeli sa vlastnosti základnej látky menia šitím. Golier a manžety bývajú dvojité alebo vystužené, léga má viac vrstiev, nite môžu mať iné zloženie a gombík reaguje na teplo inak než textil. Aj keď metráž prejde skúškou prania, hotový odev môže zvlniť výstuž, skrútiť šev alebo zmeniť pomer dĺžok jednotlivých vrstiev.",
    ],
    "table1_heading": "Chambray, denim, popelín a Oxford: čo sa skutočne líši",
    "table1_intro": "Názvy košeľových tkanín sa v predaji používajú voľne. Rozlíšenie väzby a priadzí je užitočné, ale povolenú údržbu vždy určuje konkrétny hotový výrobok.",
    "table1_headers": ["Tkanina", "Typická väzba a farba", "Povrch", "Dôležité pri praní"],
    "table1_rows": [
        ("Chambray", "Najčastejšie plátnová väzba, farebná jedna sústava a svetlá druhá.", "Jemný krížový melír bez dominantnej diagonály.", "Stálofarebnosť, golier, manžety, zrazenie a nízke trenie."),
        ("Denim", "Najčastejšie osnovný keper s farbenou osnovou a svetlým útkom.", "Viditeľnejšia diagonálna línia a rozdiel líca od rubu.", "Oter farbiva, hmotnosť, kovové prvky a konkrétne dokončenie."),
        ("Popelín", "Hustá plátnová konštrukcia z jemnejších priadzí, často kusovo farbená.", "Hladší, čistejší a kompaktnejší povrch.", "Krčivosť, potlač, výstuž košele a žehlenie."),
        ("Oxford", "Košíková odvodenina, často dvojica osnovných nití cez jednotlivý útok.", "Zreteľnejšia zrnitá štruktúra a mäkší objem.", "Posun priadzí, oder goliera, dôkladný oplach a tvar."),
        ("Imitácia chambray", "Potlač alebo priadze rovnakej farby vytvoria podobný vzhľad.", "Efekt môže byť hlavne na líci bez typického miešania z rubu.", "Riaďte sa potlačou a zložením, nie marketingovým názvom."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať chambray od tenkého denimu doma",
            "paragraphs": [
                f"Položte látku na biely podklad a pozrite sa lupou na kríženie nití. Pravidelný rytmus jedna nad a jedna pod smeruje k plátnovej väzbe; diagonálne prebiehajúce väzné body smerujú ku kepru. Praktické súvislosti pri zmesových denimových odevoch rozoberá článok <a href=\"{ARTICLE_DENIM}\">o bavlne a elastane v rifliach</a>. Identifikácia podľa fotografie bez rubu môže byť nepresná.",
                "Porovnajte líc a rub, hranu šva a voľný koniec nite. Pri typickom modro-bielom chambray uvidíte farebnú priadzu v jednom smere a svetlú v druhom. Pri tenkom denime môže byť rub výrazne svetlejší a diagonála čitateľnejšia. Ani jeden znak však neurčuje bavlnu, indigo, zmes alebo povolenie sušičky; tieto údaje hľadajte osobitne.",
            ],
        },
        {
            "heading": "Prečo nový modrý chambray môže púšťať farbu",
            "paragraphs": [
                f"Farba sa môže uvoľniť pre nedostatočnú fixáciu, prebytok nespojeného farbiva alebo povrchové farbenie priadze, ktoré sa používaním prirodzene otiera. Mechanizmy vysvetľuje článok <a href=\"{ARTICLE_COLOR}\">prečo textil bledne pri praní, svetle a trení</a>. Tmavý mokrý kus preto nedávajte na svetlé čalúnenie a prvé cykly oddeľte podľa pokynov výrobcu.",
                "Domáca skúška bielou navlhčenou handričkou môže upozorniť na prenos, ale nepredpovedá presne päť praní ani trenie pri nosení. Príliš dlhé namáčanie, zásaditý produkt, vysoká teplota a drhnutie môžu výsledok zhoršiť. Ak nová košeľa farbí pokožku alebo ostatnú bielizeň nad rámec uvedeného návodu, zdokumentujte stav a riešte ho s predajcom.",
            ],
        },
        {
            "heading": "Ako vyčistiť golier, manžety a podpazušie chambray košele",
            "paragraphs": [
                "Golier a manžety zhromažďujú kožný maz, pot, kozmetiku a prach. Košeľu rozložte, škvrnu najprv odsajte a malé množstvo kompatibilného prípravku naneste podľa jeho návodu. Pracujte prstami alebo mäkkou hladkou plochou bez tvrdej kefy. Dlhé agresívne trenie mení farbu aj odraz a na melírovanom podklade vytvorí svetlý oblúk.",
                "Dezodorant môže vytvoriť tuhú bielu vrstvu, zatiaľ čo starý pot mení farbivo alebo oslabuje vlákno. Zvyšok produktu najprv rozpustite a opláchnite spôsobom povoleným etiketou; neskladajte naň ďalšie domáce kyseliny, zásady a bielidlo. Pri citlivom alebo kontrastnom šve urobte skúšku na vnútornej záložke a miesto posúďte až suché.",
            ],
        },
        {
            "heading": "Ako prať chambray košeľu v práčke",
            "paragraphs": [
                f"Najprv prečítajte <a href=\"{ARTICLE_LABEL}\">materiálový a ošetrovací štítok</a>. Vyprázdnite vrecká, odstráňte odnímateľné výstuže goliera a zapnite iba prvky, ktoré sa podľa výrobcu nemajú voľne zachytiť; gombíkovú légu spravidla nenaťahujte úplným zapnutím, ak by sa pri pohybe namáhala. Košeľu obráťte naruby, ale škvrny predtým lokalizujte.",
                "Perte s podobnými farbami a podobne ľahkou hladkou bielizňou. Zipsy, háčiky a hrubé uteráky zvyšujú bodový oder. Bubon nepreplňte, pretože stlačené košele sa horšie opláchnu a záhyby sa trú na rovnakom mieste. Program, teplotu, otáčky a prípadný ochranný vak zvoľte podľa etikety, nie podľa všeobecnej predstavy o bavlne.",
            ],
        },
        {
            "heading": "Ručné pranie chambray nie je automaticky šetrnejšie",
            "paragraphs": [
                "Ručné pranie dáva kontrolu nad pohybom, ale zároveň zvádza k dlhému namáčaniu, drhnutiu goliera a krúteniu mokrej košele. V nádobe musí byť dosť priestoru, aby sa voda vymenila medzi vrstvami. Odev jemne ponárajte a podopierajte; nesnažte sa z neho vytlačiť vodu stáčaním do povrazu.",
                "Teplotu udržujte v rámci symbolu a medzi praním a oplachom nevytvárajte prudký tepelný rozdiel. Produkt najprv rozptýľte vo vode podľa návodu. Ak modrá voda stále výrazne farbí aj po odporúčanom postupe, nepridávajte naslepo soľ, ocot alebo ďalší gél. Farbiaci mechanizmus a kompatibilitu domáca zmes spoľahlivo neurčí.",
            ],
        },
        {
            "heading": "Škvrny od oleja, kávy, pera a jedla na chambray",
            "paragraphs": [
                f"Každú škvrnu začnite odstránením prebytku bez rozširovania. Olej odsajte, pevnú nečistotu zdvihnite tupou hranou a tekutinu prikladajte bielym savým materiálom. Rozdelenie podľa typu nájdete v návode <a href=\"{ARTICLE_STAIN}\">ako odstraňovať rôzne škvrny z oblečenia</a>. Farebnú košeľu neprelievajte chlórom ani univerzálnym odfarbovačom.",
                "Pri pere a make-upe je rozpúšťadlo aj nosič pigmentu rozhodujúci. Náhodné trenie alkoholom môže rozpustiť škvrnu, ale zároveň vytiahnuť farbivo a vytvoriť kruh. Vyskúšajte kompatibilný produkt na skrytom mieste, pracujte od okraja ku stredu a celú ošetrenú zónu po povolenom postupe rovnomerne opláchnite. Žehlenie odložte, kým mastný či farebný zvyšok nezmizne.",
            ],
        },
        {
            "heading": "Sušenie bez skrúteného šva a tvrdého goliera",
            "paragraphs": [
                f"Košeľu vyberte po skončení cyklu a jemne pretrepte bez prudkého švihu. Urovnajte légu, bočné švy, golier a manžety do prirodzeného tvaru. Súvislosti rozmerovej zmeny vysvetľuje článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie po praní zráža</a>. Mokrý kus nenaťahujte na pôvodné číslo, kým neviete, či sa zmenila látka, niť alebo výstuž.",
                "Sušte v tieni s prúdením vzduchu na primerane širokom vešiaku alebo spôsobom zo štítku. Úzky drôt môže vytlačiť ramená a dva kolíky neúmerne zaťažiť lem. Golier nechajte otvorený, aby vyschol aj pod výstužou. Sušičku použite iba pri výslovnom povolení; prevaľovanie a teplo urýchľujú oder hrán aj zmenu rozmeru.",
            ],
        },
        {
            "heading": "Ako žehliť chambray a zachovať prirodzený melír",
            "paragraphs": [
                f"Žehlenie začnite ešte pred úplným presušením iba vtedy, keď to zloženie a symbol dovoľujú. Praktické poradie dielov ponúka návod <a href=\"{ARTICLE_IRONING}\">ako vyžehliť košeľu</a>. Pri chambray navyše žehlite z rubu alebo cez čistú ochrannú tkaninu, aby ste na farebnej osnove nevytvorili lesklý pás či odtlačok švovej rezervy.",
                "Golier, manžetu a légu rozložte bez preťahovania. Nad výšivkou, potlačou, elastickou niťou a plastovým gombíkom znížte teplotu alebo sa im vyhnite podľa pokynu. Para môže uvoľniť záhyb, ale na nestálofarebnom alebo nerovnomerne navlhčenom kuse vytvoriť mapu. Skúšobné miesto nechajte vychladnúť a až potom hodnotíte lesk.",
            ],
        },
        {
            "heading": "Svetlé hrany, vyblednutie a prirodzená patina",
            "paragraphs": [
                "Na modrom chambray sa ako prvé menia hrany goliera, lakte, švy a miesta pod popruhom. Môže ísť o úbytok povrchového farbiva, zbrúsenie vystupujúcich vlákien alebo trvalý lom. Ak sa jav mení podľa uhla svetla, význam má aj zmena povrchovej geometrie. Ďalšie pranie tieto mechanické rozdiely nevyrovná a môže ich zvýrazniť.",
                "Pred zásahom porovnajte symetrické miesto a rub. Jemnú patinu možno prijať ako vlastnosť, no ostrá nová škvrna po jednom cykle potrebuje zdokumentovať použitý program, prostriedok a náplň. Farbivo sa nesnažte lokálne dopĺňať fixkou alebo neznámym farbiacim sprejom; rozdielny odtieň a stálosť komplikujú neskoršiu opravu.",
            ],
        },
        {
            "heading": "Ako skladovať chambray košele a šaty",
            "paragraphs": [
                "Odev uložte úplne čistý a suchý. Košeľu zaveste na vešiak, ktorý podopiera ramená, alebo ju zložte s voľným lomom mimo trvalého tlaku. Golier nestláčajte pod ťažký predmet a gombíky nenechajte odtlačené do lícnej strany. Pri tmavom nestálofarebnom kuse oddeľte priamy kontakt so svetlou citlivou textíliou.",
                "Dlhodobé priame slnko vyťahuje farbu nerovnomerne a uzavretý vlhký priestor podporuje zatuchnutie. Vôňa nenahrádza odstránenie potu ani sušenie. Pred sezónnym uložením skontrolujte podpazušie, golier a vnútro vreciek; neviditeľný maz časom oxiduje a viaže prach. Po dlhom uložení odev najprv vyvetrajte a prezrite, nie automaticky preperte.",
            ],
        },
        {
            "heading": "Ako vybrať chambray košeľu alebo metráž podľa použitia",
            "paragraphs": [
                f"Pri košeli sledujte zloženie, hustotu, priesvitnosť, švy, výstuž a symboly. Porovnanie s hladkou košeľovinou dopĺňa článok <a href=\"{ARTICLE_POPLIN}\">čo je popelín</a>. Na pracovnú zásteru môže byť dôležitejšia odolnosť farby a pranie, na letnú blúzku hmotnosť a priedušnosť, na šaty splývavosť a podšívka. Jeden mäkký dotyk nevysvetľuje budúcu stabilitu.",
                "Pri metráži si vyžiadajte zloženie, plošnú hmotnosť, odporúčané predpranie, rozmerovú zmenu a informáciu o farbení. Odstrih skúšobne začistite a operte plánovaným spôsobom. Po vysušení zmerajte osnovný aj útkový smer, porovnajte farbu, krčivosť a švík. Až potom strihajte všetky diely; tým sa zníži riziko, že hotový odev skráti rozdielne pozdĺž a naprieč.",
            ],
        },
    ],
    "table2_heading": "Chambray po praní: čo znamená konkrétny prejav",
    "table2_intro": "Košeľu hodnotíte až suchú pri rovnakom osvetlení. Mokrá tmavne a melírovaná konštrukcia môže zdanlivú mapu zvýrazniť alebo skryť.",
    "table2_headers": ["Prejav", "Pravdepodobné vysvetlenie", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Svetlý oblúk na golieri", "Oder, zvyšok kozmetiky alebo príliš silné lokálne čistenie.", "Omak, prenos na bielu handričku a zmenu podľa svetla.", "Nepridávať trenie; pri zvyšku produktu šetrne opláchnuť podľa etikety."),
        ("Krútený bočný šev", "Skosenie, rozdielne napätie alebo rozmerová zmena.", "Smer šva na suchom odeve a rovnaký model pred praním.", "Nenapínať mokrý; zdokumentovať a pri novom kuse reklamovať."),
        ("Zvlnený golier", "Rozdielne zrazenie látky a výstuže alebo uvoľnenie lepidla.", "Lepkavosť, bubliny a symbol žehlenia.", "Nezohrievať naslepo; posúdiť krajčírom alebo predajcom."),
        ("Modrá mapa na svetlom kuse", "Prenos farbiva v zmiešanej náplni.", "Ktorý kus púšťal a či je škvrna ešte mokrá.", "Oddeliť zdroj a použiť kompatibilný postup bez horúceho sušenia."),
        ("Tvrdé pásy pri švoch", "Zvyšok gélu, minerály alebo presušený ostrý lom.", "Oplach, dávku a tvrdosť vody.", "Pri povolení zopakovať šetrný oplach bez ďalšej dávky."),
    ],
    "steps_heading": "Ako vyprať chambray košeľu krok za krokom",
    "steps": [
        "Prečítajte zloženie a všetky symboly a skontrolujte výstuž goliera, gombíky, potlač a kontrastnú niť.",
        "Pri dennom svetle označte mastný golier, podpazušie, manžety, staré mapy a poškodené švy.",
        "Urobte skúšku stálofarebnosti a škvrnu ošetrite kompatibilným postupom bez tvrdej kefy.",
        "Košeľu oddeľte od svetlej, ťažkej a ostrej bielizne a obráťte ju naruby podľa konštrukcie.",
        "Zvoľte iba program a teplotu zo štítku, primeranú náplň a presne odmeraný prostriedok.",
        "Po cykle košeľu ihneď vyberte, podoprite a urovnajte švy, golier a manžety bez násilného ťahu.",
        "Sušte v tieni s otvorenými dvojitými vrstvami a sušičku použite len pri povolenom symbole.",
        "Žehlite pri kompatibilnej teplote z rubu a po vychladnutí skontrolujte farbu, rozmery aj výstuž.",
    ],
    "remember": [
        "Je tkanina skutočne plátnový chambray, alebo tenký denim, Oxford či potlačená imitácia?",
        "Aké vlákno, elastan, potlač, niť, gombík a výstuž určujú najnižší limit?",
        "Je svetlé miesto škvrna, zvyšok produktu, oder farbiva alebo lesk po teple?",
        "Púšťa nový odtieň farbu a je náplň oddelená od svetlých textílií?",
        "Má košeľa pri praní aj oplachu dosť priestoru bez kontaktu so zipsami?",
        "Je po sušení suchý aj golier, manžeta, léga, šev a vnútro vrecka?",
    ],
    "mistakes": [
        "Zameniť chambray s denimom a preniesť naň každý postup určený pre rifle.",
        "Predpokladať stopercentnú bavlnu iba podľa modro-bieleho vzhľadu.",
        "Drhnúť golier tvrdou kefou, kým vznikne svetlejší hladký oblúk.",
        "Prať nový tmavý kus so svetlou bielizňou bez skúšky a pokynov výrobcu.",
        "Nechať košeľu stlačenú po cykle a potom ostré lomy riešiť najvyšším teplom.",
        "Napínať mokrý skrútený šev alebo prehrievať zvlnenú lepenú výstuž.",
    ],
    "expert_heading": "Odbornejší pohľad: plátnová väzba, farbené priadze a skúšky košeľovín",
    "expert": [
        "CottonWorks opisuje plátnovú väzbu ako pravidelné striedanie väzných bodov a uvádza plain-weave chambray oddelene od keprových denimových konštrukcií. Farebná osnova so svetlým útkom mení optický dojem, nie základný fakt, že mechanické vlastnosti závisia aj od jemnosti, zákrutu, hustoty a dokončenia priadzí.",
        "ASTM D3775 meria počet osnovných a útkových nití, ASTM D1424 pokračovanie trhliny a ASTM D1683 správanie šitého šva. Ide o odlišné vlastnosti. Hustejšia látka nemusí mať lepší golier po nevhodnom podlepení a vysoká pevnosť plochy neznamená, že sa priadza neposunie pri nevhodnom stehu alebo tesnom strihu.",
        "AATCC TM61 hodnotí stálofarebnosť pri definovanom zrýchlenom praní a TM135 rozmerovú zmenu po definovaných domácich postupoch. Výsledok má význam len s uvedenými podmienkami. Spotrebiteľ z neho nemôže odvodiť ľubovoľnú teplotu pre inú košeľu; užitočné sú porovnateľné údaje konkrétneho výrobku a symboly celého odevu.",
    ],
    "source_intro": "Zdroje podporujú rozdiel medzi plátnovým chambray a keprovým denimom, význam väzby, farby, šva a rozmerovej zmeny. Nepodporujú jednu univerzálnu teplotu pre všetky košele označené chambray.",
    "sources": [
        ("CottonWorks: denimové a plain-weave chambray konštrukcie", COTTONWORKS_DENIM),
        ("CottonWorks: základné tkané väzby", COTTONWORKS_BASIC_WEAVES),
        ("CottonWorks: kvalita priadze a tkaniny", COTTONWORKS_QUALITY),
        ("ASTM D3775: počet osnovných a útkových nití", ASTM_COUNT),
        ("ASTM D1424: pokračovanie trhliny", ASTM_TEAR),
        ("ASTM D1683: zlyhanie šitých švov tkanín", ASTM_SEAM),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("AATCC TM135: rozmerové zmeny po praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Bavlna a elastan v rifliach", ARTICLE_DENIM),
        ("Čo je popelín a ako sa oň starať", ARTICLE_POPLIN),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Prečo farby blednú pri praní a trení", ARTICLE_COLOR),
        ("Prečo sa oblečenie po praní zráža", ARTICLE_SHRINKAGE),
        ("Ako vyžehliť košeľu", ARTICLE_IRONING),
    ],
    "faq_title": "chambray košele, šaty a metráž",
    "faq": [
        ("Čo je chambray?", "Najčastejšie ide o ľahšiu plátnovo tkanú látku s farebnou jednou sústavou nití a svetlou druhou, ktorá vytvára jemný melír."),
        ("Je chambray to isté ako denim?", "Nie. Typický chambray má plátnovú väzbu, kým klasický denim osnovný keper s viditeľnou diagonálou."),
        ("Je chambray vždy bavlna?", "Nie. Môže obsahovať polyester, ľan, viskózu, elastan alebo inú zmes, preto treba čítať percentá na etikete."),
        ("Môže sa chambray prať v práčke?", "Áno iba vtedy, keď to povoľuje etiketa celého výrobku vrátane výstuže, potlače a ozdôb."),
        ("Na koľko stupňov prať chambray košeľu?", "Jedna teplota neexistuje. Rozhoduje zloženie, farbenie, úprava a symbol konkrétneho odevu."),
        ("Púšťa chambray farbu?", "Tmavý alebo indigom farbený kus môže uvoľňovať povrchové farbivo. Overte pokyny a perte ho s podobnými farbami."),
        ("Ako odstrániť mastný golier?", "Prebytok odsajte, použite kompatibilný prípravok po skrytej skúške a obmedzte trenie, aby nevznikol svetlý oblúk."),
        ("Treba košeľu obrátiť naruby?", "Často to znižuje priamy oder líca, ale postup musí zostať kompatibilný s potlačou, gombíkmi a pokynom výrobcu."),
        ("Môže ísť chambray do sušičky?", "Len pri výslovnom symbole. Teplo a prevaľovanie môžu zmeniť farbu, rozmer, elastan aj výstuž."),
        ("Ako žehliť chambray?", "Podľa symbolu, ideálne z rubu alebo cez ochrannú tkaninu, s teplotou najcitlivejšej zložky a malým tlakom."),
        ("Prečo sa bočný šev po praní skrútil?", "Príčinou môže byť skosenie tkaniny, napätie pri šití alebo rozdielna rozmerová zmena. Hodnoťte ho až suchý."),
        ("Ako chambray skladovať?", "Úplne čistý a suchý, na vhodnom vešiaku alebo voľne zložený, mimo slnka, vlhkosti a ostrého dlhodobého tlaku."),
        ("Je vyšší počet nití vždy lepší?", "Nie. Hustota je iba jeden parameter; dôležité sú priadze, väzba, farba, šev, dokončenie a určenie výrobku."),
    ],
}

commercial(
    CHAMBRAY,
    noun="chambray odev",
    limit=(
        "Gél nie je automaticky vhodný na hodvábnu, vlnenú alebo nestálofarebnú zmes, "
        "lepené sako ani odev určený na profesionálne čistenie. Neobnoví odreté farbivo, "
        "skrútený šev alebo zvlnenú výstuž."
    ),
)


MATELASSE: dict[str, object] = {
    "title": "Čo je matelassé: plastický tkaný reliéf, zrážanie a starostlivosť",
    "link": "co-je-matelasse-plasticky-tkany-relief-zrazanie-a-starostlivost",
    "meta": "Čo je matelassé, ako sa líši od prešívaného paplóna a damasku a ako prať, sušiť a žehliť reliéfne prehozy a odevy bez sploštenia vzoru.",
    "short": "Matelassé je označenie textilu s plastickým prešívaným vzhľadom; pri tkanej verzii vzniká reliéf už väzbou viacerých sústav nití a nemusí obsahovať samostatnú výplň. Údržbu určuje presná konštrukcia a celé zloženie.",
    "name": "matelassé",
    "locative": "matelassé",
    "identity_heading": "Matelassé môže byť tkaný efekt bez vloženej výplne",
    "identity_detail": "Pri klasickej tkanej konštrukcii spolupracuje lícna a rubová sústava, väzné alebo sťahujúce nite a niekedy hrubšia vložená priadza, aby vznikli vyvýšené a vtiahnuté plochy podobné prešívaniu.",
    "identity_boundary": "V obchode sa rovnaké slovo môže použiť aj na skutočne prešívaný výrobok s vrchnou látkou, výplňou a rubom, preto treba overiť, či reliéf tvorí väzba alebo samostatne zošitý viacvrstvový systém.",
    "label_focus": "všetky deklarované vlákna, počet vrstiev, väzné a výplňové priadze, podšívku, výstuž, ozdobné nite, zips, lem, povrchovú úpravu a povolené sušenie a žehlenie",
    "missing_label": "Pri metráži si vyžiadajte technické údaje o konštrukcii a rozmerovej zmene; pri prehoze bez etikety nerozhodujte podľa toho, že na reze nevidíte voľnú výplň.",
    "dry_check": "sploštené vrcholy reliéfu, pretrhnuté väzné nite, rozostúpené švy, ostré trvalé sklady, nerovnomerné vydutie, mapy od vody, poškodený lem a známky oddelenia vrstiev",
    "damage_boundary": "Povrchovú nečistotu možno odstrániť, ale prasknutá sťahujúca niť, deformované vrecko reliéfu alebo teplom sploštený vzor sa ďalším praním spravidla nevrátia.",
    "test_focus": "Skúšku pozorujte pri bočnom svetle a z oboch strán, pretože lokálne navlhčenie môže dočasne zmeniť výšku, smer odrazu aj napätie medzi lícom a rubom.",
    "combined_risk": "rozdielneho napučania viacerých sústav nití, uvoľnenia sťahujúcej konštrukcie, tlaku na vyvýšený vzor a nerovnomerného schnutia v plastických zónach",
    "chemistry_boundary": "Škvrna na vrchole a zvyšok produktu v priehlbine potrebujú odlišné odsatie; koncentrovaná chémia sa môže zachytiť medzi vrstvami a po vyschnutí vytvoriť tuhú mapu.",
    "drying_detail": "Prehoz rozložte tak, aby vzduch dosiahol líc, rub, vyvýšené bunky, lem a prípadnú podšívku; odev podoprite podľa švov a neponechajte reliéf stlačený mokrou hmotnosťou.",
    "heat_boundary": "Horúci radiátor, sušička alebo silný tlak žehličky môžu zmeniť rozmer jednotlivých sústav, zataviť syntetickú niť, sploštiť reliéf alebo zafixovať ostrý odtlačok.",
    "stop_signs": "rastúce bubliny, praskanie väzných nití, lepkavosť, sťahovanie iba jednej vrstvy, prenos farby, tvrdnúca mapa, zápach z vnútra alebo deformácia lemu",
    "professional_boundary": "Moderný bavlnený prateľný prehoz možno často ošetrovať doma podľa etikety, kým hodvábny, historický, kovom zdobený, podšitý alebo skutočne viacvrstvový kus môže vyžadovať profesionálne či konzervačné čistenie.",
    "answer": "Matelassé je textil s plastickým vzhľadom pripomínajúcim prešívanú látku. Pri tkanej verzii vzniká reliéf už na stave pomocou viacerých sústav nití a väzobného napätia, takže medzi lícom a rubom nemusí byť samostatná mäkká výplň. V predaji však môže názov označovať aj skutočne prešívaný viacvrstvový výrobok. Pred praním preto skontrolujte rez, rub, švy, etiketu a najcitlivejšiu vrstvu. Prateľný prehoz perte s dostatkom priestoru, bez tvrdých zipsov a presne nadávkovaným kompatibilným prostriedkom. Mokré matelassé nekrúťte, sušte rovnomerne z oboch strán a reliéf nežehlite naplocho vysokým tlakom.",
    "intro": "Plastický prehoz alebo sako môže vyzerať ako tenko vypchaté, hoci jeho objem vytvorila samotná väzba. Práve táto vizuálna podobnosť vedie k chybným návodom: niekto použije postup pre paplón s výplňou, iný pre hladký damask a tretí stlačí povrch žehličkou. Bezpečná starostlivosť o matelassé začína určením skutočnej konštrukcie. Potom sa posudzuje zloženie všetkých priadzí, napätie medzi lícom a rubom, rozmer, škvrna, sušenie plastických zón a hranica medzi čistiteľnou mapou a mechanickou stratou reliéfu.",
    "quick": [
        "<strong>Reliéf nemusí byť výplň:</strong> tkané matelassé môže napodobniť prešívanie bez samostatného vatelínu.",
        "<strong>Názov sa používa voľne:</strong> pri hotovom výrobku overte, či je tkaný, prešívaný, pletený alebo tepelne tvarovaný.",
        "<strong>Najnižší limit rozhoduje:</strong> líc, rub, väzné nite, ozdoby a podšívka môžu mať rozdielne zloženie.",
        "<strong>Reliéf chráňte pred tlakom:</strong> preplnený bubon, štipce a horúca žehlička môžu vzor sploštiť.",
        "<strong>Sušte cez celú hrúbku:</strong> priehlbiny, lem a viac vrstiev zostávajú vlhké dlhšie než vrcholy.",
        "<strong>Prasknutá väzná niť nie je škvrna:</strong> pranie nevytvorí späť pôvodné napätie ani pravidelnú bunku.",
    ],
    "overview_heading": "Ako tkané matelassé vytvára objem bez klasického prešívania",
    "overview": [
        "Historické odborné príručky opisujú matelassé ako odvodeninu viacvrstvových tkanín, pri ktorej lícna a rubová konštrukcia spolupracujú so sťahujúcimi alebo väznými priadzami. Rozdiel v napätí a dĺžke väzných úsekov vytvára oblasti, ktoré vystúpia a susedné línie sa vtiahnu. Figúra môže vzniknúť kombináciou keprovej, košíkovej alebo inej vzorovej väzby bez ihly prechádzajúcej hotovým sendvičom.",
        "Súčasné výrobky môžu používať jednoduchšie dvojité tkanie, hrubšiu stredovú priadzu, žakárové riadenie, pletenú štruktúru alebo následnú tepelnú úpravu. Preto nemožno z fotografie rozhodnúť, či medzi vrstvami naozaj niečo je. Skontrolujte rub, šev, okraj a technický opis. Ak sa reliéf tvorí samostatným prešívaním cez výplň, starostlivosť musí riešiť aj migráciu a sušenie tejto výplne.",
        "Plastický povrch mení kontakt s okolím. Vrcholy prijímajú viac oderu a tlaku, priehlbiny zachytávajú prach a prostriedok, zatiaľ čo spojovacie body nesú rozdiel napätí medzi plochami. Čistenie preto nemá za cieľ vyrovnať povrch do hladka. Cieľom je odstrániť nečistotu pri zachovaní navrhnutého rozdielu výšok, rozmeru a súdržnosti vrstiev.",
    ],
    "table1_heading": "Matelassé, prešívaný textil, damask a reliéfna potlač",
    "table1_intro": "Podobný vzhľad môže vzniknúť štyrmi odlišnými spôsobmi. Okraj, rub a rez švu často povedia viac než fotografia líca.",
    "table1_headers": ["Konštrukcia", "Ako vzniká plastika", "Čo hľadať", "Hlavné riziko"],
    "table1_rows": [
        ("Tkané matelassé", "Viac sústav nití, väzba a rozdielne napätie vytvoria vyvýšené polia.", "Súvislý tkaný rub, väzné body a reliéf bez ihlového stehu cez hotový sendvič.", "Rozdielne zrazenie, prasknutá väzná niť a sploštenie."),
        ("Skutočne prešívaný výrobok", "Steh spája vrchnú látku, výplň a rub.", "Línie šitia prechádzajú vrstvami a na reze je samostatná výplň.", "Zhluknutie výplne, dlhé sušenie a pretrhnutie stehu."),
        ("Damask", "Vzor vytvára kontrast väzieb a odrazu, zvyčajne bez výrazných vypuklých buniek.", "Obrátený lesklý a matný motív na rube.", "Zachytenie dlhších flotáží a lesk po tlaku."),
        ("Tepelne tvarovaný alebo lepený reliéf", "Tvar fixuje teplo, pena, záter alebo spojivo.", "Iný rub, pravidelné zvarové body alebo vrstva medzi plochami.", "Deformácia teplom, delaminácia a lepkavosť."),
        ("Reliéfna potlač", "Objem vytvorí nános pasty alebo penivá úprava na povrchu.", "Motív je najmä na líci a rub zostáva základnou látkou.", "Praskanie, oder a nekompatibilná chémia."),
    ],
    "sections": [
        {
            "heading": "Ako doma zistiť, či ide o tkaný alebo prešívaný výrobok",
            "paragraphs": [
                "Začnite na skrytom leme alebo otvorenom okraji. Sledujte, či stehy prechádzajú cez celý výrobok, či je medzi vrstvami samostatné rúno a či sa reliéf opakuje podľa väzby nití. Tkané matelassé môže mať zložitejší rub a drobné spojovacie body, zatiaľ čo prešívaný kus ukáže niť z ihly a oddeliteľnú výplň. Nerozpárajte hotový výrobok iba kvôli skúške.",
                f"Porovnanie tkaného vzoru dopĺňa článok <a href=\"{ARTICLE_DAMASK}\">čo je damask</a>. Damask pracuje hlavne s odrazom, matelassé s výškovým efektom, no moderné názvy sa môžu prekrývať. Ak etiketa alebo technický list označí výrobok ako viacvrstvový, prijmite túto informáciu aj vtedy, keď výplň na dotyk necítite. Najcitlivejšia vnútorná časť stále určuje čistenie.",
            ],
        },
        {
            "heading": "Prečo sa reliéf po praní zmenší, zväčší alebo sploští",
            "paragraphs": [
                f"Jednotlivé sústavy priadzí mohli byť pri výrobe napnuté rozdielne. Voda a teplo uvoľnia časť vnútorného napätia a vlákna napučia či zmenia dĺžku. Výsledok môže reliéf zvýrazniť alebo stiahnuť. Mechanizmy rozoberá článok <a href=\"{ARTICLE_SHRINKAGE}\">prečo sa oblečenie zráža po praní</a>. Zmenu hodnotíte až po úplnom vysušení a ustálení.",
                "Sploštenie nemusí byť zrazenie. Mokrý prehoz mohol zostať stlačený v bubne, visieť cez ostrú hranu alebo byť žehlený z líca vysokým tlakom. Ak sú väzné nite celé, časť výšky sa po šetrnom vysušení a uvoľnení môže vrátiť. Ak však niť praskla alebo syntetický povrch zmäkol teplom, ďalšia para môže poškodenie zväčšiť.",
            ],
        },
        {
            "heading": "Ako odstrániť prach z priehlbín pred mokrým čistením",
            "paragraphs": [
                "Prehoz najprv jemne vytraste v priestore, kde sa prach nerozšíri späť do miestnosti. Mäkkou kefou pracujte bez tlaku po reliéfe a pri povolenom vysávaní použite nízky výkon cez ochrannú sieťku. Hubicu nenechajte prisať na vyvýšenú bunku, pretože môže vytiahnuť väznú niť alebo nerovnomerne stlačiť povrch.",
                "Prach zmiešaný s vodou vytvára sivú pastu v priehlbinách a na švoch. Suché odstránenie preto znižuje potrebu dlhého mokrého zásahu. Pri starom krehkom kuse, uvoľnenej niti alebo odlupujúcej sa povrchovej úprave zastavte aj vysávanie. Mechanicky nestabilný povrch patrí najprv na posúdenie, nie do bubna.",
            ],
        },
        {
            "heading": "Ako prať matelassé prehoz alebo prikrývku",
            "paragraphs": [
                f"Najprv overte rozmery, hmotnosť a symboly. Bežnú starostlivosť o posteľný textil vysvetľuje článok <a href=\"{ARTICLE_BEDDING}\">ako správne prať obliečky</a>, no reliéfny prehoz potrebuje navyše dostatočne veľký bubon a ochranu plastiky. Zipsy, háčiky a ťažké predmety oddeľte. Ak suchý prehoz bubon takmer vypĺňa, mokrý nebude mať priestor na pohyb ani oplach.",
                "Zvoľte iba povolený program a presnú dávku. Nadbytok gélu sa môže držať v priehlbinách a medzi vrstvami. Prehoz nevkladajte zrolovaný do tesného valca; voľne ho rozložte podľa pokynu zariadenia. Po cykle ho vyberte s rovnomernou oporou, nie za jeden mokrý roh, a skontrolujte, či niektorá bunka nezostala preliačená alebo premočená.",
            ],
        },
        {
            "heading": "Ako prať matelassé šaty, sukňu alebo sako",
            "paragraphs": [
                "Odev môže mať podšívku, podlep, zips, tvarované švy a ozdoby s nižším limitom než základná tkanina. Sako určené na profesionálne čistenie nevkladajte do práčky preto, že odrezok matelassé je bavlnený. Pri prateľných šatách obráťte odev podľa etikety, zabezpečte kovanie a oddeľte ho od drsných kusov, ktoré reliéf stláčajú alebo zachytávajú.",
                "Mokrý plastický odev je ťažší a jednotlivé diely môžu reagovať rozdielne podľa smeru strihu. Nezdvíhajte ho za ramienko, tenký pás alebo ozdobnú časť. Po praní podoprite švy a urovnajte tvar bez naťahovania vzoru. Pri podšívke otvorte vrecká a rozostupy, aby sa vlhkosť neuzavrela medzi vrstvami a nevytvorila mapu.",
            ],
        },
        {
            "heading": "Škvrny od nápoja, mastnoty a kozmetiky na reliéfe",
            "paragraphs": [
                f"Tekutinu odsajte bielou savou tkaninou z vrcholov aj priehlbín bez trenia. Pevný zvyšok nadvihnite tupou hranou. Postup podľa povahy škvrny rozoberá návod <a href=\"{ARTICLE_STAIN}\">ako odstraňovať rôzne škvrny z textilu</a>. Prostriedok nanášajte kontrolovane; mláka na jednej bunke môže preniknúť na rub a po vysušení vytiahnuť okraj.",
                "Pri mastnote pracujte od okraja ku stredu a kontrolujte rub, či sa škvrna neposúva medzi vrstvami. Make-up a rúž spájajú tuk a pigment, preto silné drhnutie rozšíri farebnú plochu aj zmení výšku reliéfu. Po kompatibilnom ošetrení potrebuje zóna rovnomerný oplach alebo odsatie. Žehlenie odložte, kým nie je isté, že vnútri nezostal mastný zvyšok.",
            ],
        },
        {
            "heading": "Ako sušiť matelassé bez zatuchnutia v plastických bunkách",
            "paragraphs": [
                f"Prehoz rozložte na čistú pevnú alebo dobre podopretú priedušnú plochu podľa etikety a pravidelne meňte polohu. Všeobecné zásady prúdenia vzduchu nájdete v článku <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Pri matelassé kontrolujte aj spodnú stranu, lem, hrubšie motívy a miesta, ktoré ležali na podložke.",
                "Zavesenie cez jednu šnúru môže vytvoriť ostrý prehyb a sústrediť mokrú hmotnosť do úzkej línie. Štipce nedávajte na viditeľný vrchol. Ak je povolená sušička, dodržte kapacitu a nastavenie; povrchové sucho neznamená suchú vnútornú priadzu alebo výplň. Prehoz neukladajte na posteľ, kým sa po krátkom zložení nevracia chlad ani vlhký pach.",
            ],
        },
        {
            "heading": "Ako žehliť alebo naparovať matelassé bez straty plastiky",
            "paragraphs": [
                f"Žehličku nepoložte naplocho na lícny reliéf. Ak etiketa žehlenie povoľuje, pracujte z rubu cez ochrannú tkaninu na mäkšej podložke a s čo najmenším tlakom. Základy bezpečného žehlenia dopĺňa článok <a href=\"{ARTICLE_IRONING}\">ako žehliť košeľu a textil</a>. Najprv skúste skrytý roh a nechajte ho vychladnúť.",
                "Para môže uvoľniť záhyb, ale zároveň napučať prírodnú priadzu, zmeniť sťahujúce napätie alebo ovplyvniť lepidlo a syntetickú zložku. Namiesto tlačenia nechajte paru pôsobiť v povolenej vzdialenosti a tvar podoprite. Trvalý lesklý odtlačok alebo roztavený vrchol sa ďalším naparovaním spravidla neopraví.",
            ],
        },
        {
            "heading": "Prasknutá väzná niť, vytiahnutá slučka a rozostúpený šev",
            "paragraphs": [
                f"Vytiahnutú niť neodstrihujte bez zistenia, kam pokračuje. Môže držať väčšiu reliéfnu zónu a jej prerušenie uvoľní ďalšie bunky. Súvislosti zachytenia vysvetľuje článok <a href=\"{ARTICLE_SNAGGING}\">prečo vznikajú vytiahnuté očká a nite</a>. Povrch položte bez napätia, miesto odfoťte a sledujte rub.",
                "Ak sa pri šve nite iba posunuli, okolitá plocha môže zostať celá; ak sa pretrhli, oprava potrebuje stabilizovať okraj. Krajčír alebo textilný reštaurátor zvolí techniku podľa zaťaženia a hodnoty. Kvapka tvrdého lepidla na líci mení ohyb a pri ďalšom praní vytvára ostrý bod, ktorý môže rezať susedné priadze.",
            ],
        },
        {
            "heading": "Skladovanie prehozu bez trvalého sploštenia reliéfu",
            "paragraphs": [
                "Matelassé uložte úplne čisté a suché. Veľký prehoz zložte do voľných širokých prehybov alebo zrolujte na primerané jadro, ak to konštrukcia a priestor umožňujú. Na plastický povrch neklaďte dlhodobo ťažký predmet a nesťahujte ho tesným vákuovým obalom, ktorý môže reliéf stlačiť a zadržať zvyškovú vlhkosť.",
                "Miesta prehybov občas zmeňte, najmä pri ťažšom alebo staršom kuse. Priedušný ochranný obal chráni pred prachom, no nesmie uzatvoriť vlhkosť. Pred uložením odstráňte mastnotu a biologické škvrny; vôňa ich nezastaví. Historické a hodvábne matelassé nepodkladajte kyslým papierom ani bežným farebným kartónom bez konzervačného odporúčania.",
            ],
        },
        {
            "heading": "Ako vybrať matelassé prehoz, poťah alebo odev",
            "paragraphs": [
                "Pýtajte sa na presné zloženie všetkých sústav, plošnú hmotnosť, počet vrstiev, prateľnosť, rozmerovú zmenu a určené použitie. Pri prehoze zohľadnite rozmery a kapacitu práčky ešte pred nákupom. Pri odeve sledujte podšívku, švové rezervy a to, či reliéf v napätých miestach nepraská alebo sa pri jemnom ohybe natrvalo nesploští.",
                "Vzorku pozorujte pri bočnom svetle, stlačte ju iba mierne a sledujte návrat po uvoľnení. Domáci dotyk nenahradí normované meranie, no odhalí ostré spojovacie body a slabé švy. Kvalitný výrobok má konštrukciu primeranú použitiu a zrozumiteľný návod; vysoký reliéf sám osebe neznamená vyššiu odolnosť ani lepšiu izoláciu.",
            ],
        },
    ],
    "table2_heading": "Matelassé po čistení: diagnostika reliéfu a vrstiev",
    "table2_intro": "Povrch porovnávajte pri bočnom svetle až po úplnom vysušení. Mokré bunky a rozdielna poloha dočasne menia výšku aj odtieň.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo skontrolovať", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Jedna bunka je plochá", "Tlak, prasknutá väzná niť alebo nerovnomerné navlhčenie.", "Rub, súvislosť nite a stav po úplnom vysušení.", "Nepridávať teplo; poškodenú niť posúdiť odborníkom."),
        ("Reliéf sa celkovo zvýraznil", "Uvoľnenie výrobných napätí alebo rozdielne zrazenie sústav.", "Suché rozmery, švy a údaje výrobcu.", "Nenapínať; pri novom kuse zdokumentovať zmenu."),
        ("Tvrdý tmavší okraj mapy", "Zvyšok produktu, mastnota alebo migrácia nečistoty medzi vrstvami.", "Prenos na bielu handričku a rub.", "Pri povolení kontrolovane opláchnuť bez drhnutia."),
        ("Lem sa vlní", "Rozdielna rozmerová zmena lemu, líca a rubu.", "Steh, vloženú pásku a suché rozmery.", "Nežehliť silou; riešiť konštrukciu lemu."),
        ("Vnútri ostáva vlhký pach", "Nedosušená hrubá zóna alebo skutočná výplň.", "Stred, rub, prehyby a podmienky sušenia.", "Rozložiť, zvýšiť bezpečné prúdenie vzduchu a nepoužívať."),
    ],
    "steps_heading": "Ako ošetriť prateľné matelassé krok za krokom",
    "steps": [
        "Určite, či je reliéf tkaný, prešívaný, pletený, lepený alebo tepelne tvarovaný a prečítajte všetky symboly.",
        "Skontrolujte líc, rub, spojovacie body, lem, švy, podšívku a prípadnú samostatnú výplň.",
        "Prach odstráňte nasucho a škvrnu odsajte; kompatibilný postup vyskúšajte na skrytom reliéfe.",
        "Overte kapacitu práčky a oddeľte výrobok od zipsov, háčikov a ťažkých textílií.",
        "Použite iba povolený cyklus, presnú dávku a priestor na pohyb a dôkladný oplach medzi vrstvami.",
        "Mokrý kus vyberte s rovnomernou oporou, nekrúťte ho a plastiku nestláčajte do pevného skladu.",
        "Sušte podľa etikety z oboch strán a kontrolujte lem, hrubé motívy, podšívku aj prípadnú výplň.",
        "Žehlite len pri povolení z rubu na mäkkej podložke a po vychladnutí porovnajte reliéf a rozmery.",
    ],
    "remember": [
        "Vzniká plastika tkaním, ihlovým prešívaním, pletením, lepením alebo tepelným tvarovaním?",
        "Obsahuje kus samostatnú výplň, rub, podšívku alebo ozdobnú niť s nižším limitom?",
        "Je ploché miesto iba mokré, alebo praskla väzná niť či sa povrch teplom zmenil?",
        "Má bubon dostatočnú kapacitu na mokrý prehoz a dôkladný oplach?",
        "Môže vzduch pri sušení obtekať líc, rub, lem a všetky plastické zóny?",
        "Povoľuje symbol sušičku, paru a žehlenie bez sploštenia reliéfu?",
    ],
    "mistakes": [
        "Považovať každý výrobok s názvom matelassé za tkaninu bez výplne.",
        "Vložiť veľký suchý prehoz do príliš malého bubna a ignorovať jeho mokrú hmotnosť.",
        "Drhnúť prach a škvrnu do priehlbín alebo vysávať uvoľnenú niť bez ochrany.",
        "Vyžehliť plastický vzor naplocho vysokou teplotou a tlakom z líca.",
        "Zavesiť mokrý prehoz za dva body alebo ho nechať zložený na jednej vlhkej hrane.",
        "Odstrihnúť väznú niť, ktorá drží väčšiu reliéfnu oblasť.",
    ],
    "expert_heading": "Odbornejší pohľad: dvojitá tkanina, väzné napätie a porovnateľné skúšky",
    "expert": [
        "Technický návod výrobcu tkáčskeho CAD systému Arahne rozkladá matelassé na lícnu, rubovú a spojovaciu konštrukciu a ukazuje aj úlohu hrubšieho výplňového útku. Múzejná história marseillského kordovaného prešívania zároveň ukazuje, prečo sa dnešný strojovo vytvorený matelassé vzhľad nesmie automaticky zameniť s ručne plneným a prešívaným historickým textilom.",
        "AATCC TM135 meria rozmerovú zmenu po definovaných postupoch. Pri plastickej viac-sústavovej tkanine treba merať smer, označené body a stav po kondicionovaní; lokálna výška reliéfu je iná veličina než celková dĺžka. ASTM D1336 sa zameriava na deformáciu priadzí po povrchovom trení, kým D1424 na pokračovanie trhliny. Jedno číslo preto nepopisuje celý plastický systém.",
        "Normovaný výsledok tkaniny nemožno bez ďalšieho preniesť na prehoz s lemom, podšívkou alebo výplňou. Pri výrobku rozhoduje interakcia vrstiev, švov a dokončenia. Pre spotrebiteľa je najdôležitejšie poznať skutočnú konštrukciu, symboly a kapacitu zariadenia; slovo matelassé je začiatok identifikácie, nie povolenie konkrétneho cyklu.",
    ],
    "source_intro": "Zdroje podporujú historické a technické rozlíšenie tkaného matelassé od kordovaného alebo vrstveného prešívania a vysvetľujú rozmer, deformáciu a trhanie. Nepodporujú jeden program pre všetky reliéfne výrobky.",
    "sources": [
        ("Arahne: technická konštrukcia dvojitého matelassé", MATELASSE_WEAVES),
        ("International Quilt Museum: marseillské kordované prešívanie", MARSEILLE_QUILTING),
        ("CottonWorks: odborný prehľad tkania", COTTONWORKS_WEAVING),
        ("CottonWorks: zrážanie a skosenie tkanín", COTTONWORKS_SHRINKAGE),
        ("ASTM D1336: deformácia priadzí v tkanine", ASTM_DISTORTION),
        ("ASTM D1424: pokračovanie trhliny", ASTM_TEAR),
        ("AATCC TM135: rozmerové zmeny po praní", AATCC_DIMENSION),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je damask a ako sa oň starať", ARTICLE_DAMASK),
        ("Ako správne prať obliečky", ARTICLE_BEDDING),
        ("Ako vyprat deku", ARTICLE_BLANKET),
        ("Prečo sa textil po praní zráža", ARTICLE_SHRINKAGE),
        ("Ako odstrániť rôzne škvrny", ARTICLE_STAIN),
        ("Ako sušiť bielizeň bez zatuchnutia", ARTICLE_DRYING),
    ],
    "faq_title": "matelassé, reliéfne prehozy a odevy",
    "faq": [
        ("Čo je matelassé?", "Je to označenie plastického textilu s prešívaným vzhľadom; pri tkanej verzii vytvára reliéf väzba viacerých sústav nití."),
        ("Má matelassé vždy výplň?", "Nie. Tkané matelassé môže vytvoriť objem bez samostatného vatelínu, no obchodný názov sa používa aj na skutočne prešívané výrobky."),
        ("Je matelassé to isté ako damask?", "Nie. Damask pracuje najmä s kontrastom väzieb a lesku, matelassé s výraznejším výškovým reliéfom."),
        ("Môže sa matelassé prať v práčke?", "Len ak to povoľuje etiketa celého výrobku a bubon má kapacitu na jeho mokrú hmotnosť a pohyb."),
        ("Na koľko stupňov prať matelassé?", "Jedna teplota neexistuje. Rozhodujú všetky vlákna, vrstvy, farby, lemy a symbol konkrétneho kusu."),
        ("Prečo reliéf po praní zosilnel?", "Mohli sa uvoľniť výrobné napätia alebo sa jednotlivé sústavy rozdielne zrazili. Hodnoťte až suchý ustálený kus."),
        ("Dá sa sploštený reliéf obnoviť parou?", "Iba ak to povoľuje výrobca a väzné nite nie sú poškodené. Para môže zmeniť vlákno, farbu aj lepidlo."),
        ("Ako odstrániť škvrnu z priehlbiny?", "Tekutinu odsajte bez trenia, použite kompatibilný postup po skrytej skúške a zabezpečte rovnomerný oplach alebo odsatie."),
        ("Môže ísť matelassé do sušičky?", "Len pri výslovnom symbole. Teplo, prevaľovanie a nedostatočná kapacita môžu sploštiť reliéf alebo zmeniť vrstvy."),
        ("Ako žehliť matelassé?", "Pri povolení z rubu, cez ochrannú tkaninu, na mäkkej podložke a s minimálnym tlakom."),
        ("Čo robiť s vytiahnutou väznou niťou?", "Neodstrihujte ju. Položte povrch bez napätia, odfoťte rub a zverte opravu človeku, ktorý rozumie väzbe."),
        ("Ako skladovať matelassé prehoz?", "Čistý a úplne suchý, vo voľných širokých prehyboch alebo zrolovaný, bez ťažkého tlaku a tesného vlhkého obalu."),
        ("Je výraznejší reliéf kvalitnejší?", "Nie automaticky. Kvalitu určuje súlad priadzí, väzby, švov, stability, určenia a zrozumiteľnej údržby."),
    ],
}

commercial(
    MATELASSE,
    noun="matelassé výrobok",
    limit=(
        "Gél nie je automaticky vhodný na hodváb, vlnu, kovové nite, lepený reliéf, "
        "historický textil ani skutočne prešívané jadro bez povolenia výrobcu. Neopraví "
        "prasknutú väznú niť alebo teplom sploštenú plastiku."
    ),
)


CHENILLE: dict[str, object] = {
    "title": "Čo je ženilka: vlasová priadza, uvoľňovanie chĺpkov a čistenie",
    "link": "co-je-zenilka-vlasova-priadza-uvolnovanie-chlpkov-a-cistenie",
    "meta": "Čo je ženilka alebo chenille, prečo púšťa chĺpky a ako bezpečne čistiť ženilkovú deku, sveter či sedačku bez vytrhnutia vlasovej priadze.",
    "short": "Ženilka je efektná priadza s krátkymi vlasovými vláknami zachytenými medzi nosnými niťami. Mäkký povrch môže meniť smer, uvoľňovať chĺpky a citlivo reagovať na oder, tlak a premočenie.",
    "name": "ženilka",
    "locative": "ženilke",
    "identity_heading": "Ženilka je vlasová priadza, nie jedno konkrétne vlákno",
    "identity_detail": "Moderná ženilková priadza má krátke chumáčiky alebo vlákna uložené kolmo okolo jadra z nosných priadzí, takže na pohľad pripomína mäkký pásik s hustým vlasom.",
    "identity_boundary": "Jadro aj vlas môžu byť z bavlny, viskózy, polyesteru, akrylu alebo zmesi a priadza môže byť zatkaná, zapletená či použitá iba ako efekt v čalúnnickej konštrukcii.",
    "label_focus": "zloženie vlasu a jadra, tkanú alebo pletenú nosnú konštrukciu, podklad, záter, lepidlo, snímateľnosť poťahu, podšívku, výplň, strapce, zips a presný čistiaci kód",
    "missing_label": "Pri klbku hľadajte pásku výrobcu a pri nábytku technický list poťahu; bez nich neodvodzujte prateľnosť podľa mäkkosti ani podľa toho, že vankúš má zips.",
    "dry_check": "riedky vlas, lysé línie, voľné chumáče, vytiahnutú celú priadzu, poškodený šev, stopy po pazúroch, zlepené miesta, mapu od vody, oder na hrane a prach v hĺbke",
    "damage_boundary": "Voľný prach a cudzie vlákna možno odstrániť, ale vytrhnutý vlas z jadra, prerezaná nosná niť alebo otvorená tkanina nie sú žmolok na oholenie.",
    "test_focus": "Pri skúške sledujte smer vlasu, prenos farby, uvoľňovanie chumáčikov a tvrdý okraj až po úplnom vysušení; mokrá ženilka vyzerá tmavšia a hladšia aj bez trvalého poškodenia.",
    "combined_risk": "zmáčania veľkého povrchu jemných vlákien, mechanického vytiahnutia vlasu z jadra, trenia o susedné slučky a pomalého schnutia podkladu alebo výplne",
    "chemistry_boundary": "Tenzid môže pomôcť odstrániť mastnotu, ale nadbytok zlepuje vlas a pri čalúnení preniká do podkladu; univerzálny domáci roztok nemožno považovať za kompatibilný s každým farbivom a lepidlom.",
    "drying_detail": "Deku alebo odev podoprite a sušte tak, aby sa mokrý vlas nestlačil do jednej plochy; pri snímateľnom poťahu kontrolujte švy a rub, pri sedačke aj penu a vnútornú hrúbku.",
    "heat_boundary": "Horúca sušička, fén, radiátor alebo žehlička môžu zataviť syntetický vlas, zmeniť viskózu, zraziť bavlnu, uvoľniť podklad a natrvalo vytvoriť lesklú hladkú stopu.",
    "stop_signs": "výrazné púšťanie farby, rastúca lysá plocha, vyťahovanie nosnej priadze, rozpad podkladu, lepkavosť, mapa na okraji, vlhký pach z výplne alebo deformácia poťahu",
    "professional_boundary": "Prateľnú ženilkovú deku či snímateľný poťah možno ošetrovať podľa ich etikety, zatiaľ čo neodnímateľná sedačka, zložité sako, viskózový vlas, historický kus alebo hlboké premočenie vyžadujú postup výrobcu alebo odborné čistenie.",
    "answer": "Ženilka, často označená aj ako chenille, je efektná priadza s krátkym mäkkým vlasom zachyteným medzi nosnými niťami. Nie je to jedno vlákno: môže byť bavlnená, viskózová, polyesterová, akrylová alebo zmesová a môže tvoriť deku, sveter, záves aj poťah sedačky. Pred čistením odlíšte voľný chĺpok, žmolok, vytiahnutú priadzu a skutočnú lysú plochu. Prach odstráňte jemne nasucho. Vodu použite iba podľa etikety konkrétneho výrobku; neodnímateľný poťah neperte ako deku. Prateľný kus chráňte pred zipsami a vysokým trením, presne dávkujte prostriedok, nekrúťte ho a úplne vysušte bez horúceho priameho vzduchu.",
    "intro": "Mäkký ženilkový povrch zvádza k intenzívnemu kefovaniu, vysávaniu rotačnou kefou alebo k domnienke, že každý uvoľnený chumáč je neškodný žmolok. Priadza je však vytvorená tak, že krátky vlas drží medzi nosnými niťami. Keď sa poškodí jadro alebo sa vlas vytrhne na opakovane namáhanom mieste, pranie ho nedoplní. Zároveň sa pod názvom ženilka predáva ľahká pletená priadza na deky, tkaný záves aj pevný poťah nalepený na podklad. Správny návod preto musí oddeliť priadzu, nosnú látku, hotový výrobok a konkrétny typ nečistoty.",
    "quick": [
        "<strong>Ženilka opisuje priadzu:</strong> mäkký vlas držia nosné nite a presné vlákno treba čítať osobitne.",
        "<strong>Chumáč nie je vždy žmolok:</strong> neodstrihujte nič, čo pri jemnom pohybe ťahá dlhšiu priadzu alebo okolie.",
        "<strong>Prach odstráňte pred vodou:</strong> nízke sanie a hladká ochrana sú bezpečnejšie než rotačná kefa.",
        "<strong>Sedačka nie je deka:</strong> zips na poťahu automaticky neznamená prateľnosť ani jednoduché nasadenie po zrazení.",
        "<strong>Nadbytok produktu zlepuje vlas:</strong> presná dávka a rovnomerné opláchnutie sú dôležitejšie než silná pena.",
        "<strong>Teplo mení lesk a omak:</strong> mokrý vlas nesušte fénom ani nežehlite naplocho bez výslovného povolenia.",
    ],
    "overview_heading": "Ako vzniká ženilková priadza a prečo má mäkký vlas",
    "overview": [
        "CottonWorks opisuje ženilku ako vlasovú priadzu, ktorá sa historicky vytvárala z rozrezaného tkaného vlasu a dnes sa bežne vyrába na špecializovaných strojoch. Odborný materiál o priadzach vysvetľuje, že chumáčiky krátkych vlákien držia medzi skanými nosnými priadzami jadra. Mäkkosť teda vzniká množstvom koncov orientovaných smerom od osi, nie tým, že by celá niť bola jeden súvislý chlpatý filament.",
        "Výrobca môže meniť dĺžku a hustotu vlasu, počet a zákrut nosných nití, zloženie, hrúbku aj spôsob zakomponovania do látky. Bavlnený vlas na polyesterovom jadre sa môže správať inak než viskózová ženilka alebo plne polyesterová priadza. Názov preto nevysvetľuje savosť, mokrú pevnosť, stálofarebnosť ani teplotu, pri ktorej sa povrch začne deformovať.",
        "Keď sa ženilka zatká do poťahu, nosná tkanina, väzba, hustota a podklad určujú, ako sa zaťaženie prenáša. Pri pletenej deke držia tvar očká a mokrý kus sa môže natiahnuť. Pri čalúnení je povrch napnutý na pene a ráme. Rovnaký mäkký vlas tak potrebuje tri rozdielne stratégie: podopreté pranie, lokálne výrobcom povolené čistenie alebo odborný zásah.",
    ],
    "table1_heading": "Ženilka, buklé, zamat a plyš: rozdiel v povrchu",
    "table1_intro": "Maloobchodné názvy sa môžu miešať. Pozrite sa na jednu priadzu, rub a nosnú konštrukciu, no čistenie aj tak riaďte etiketou celého predmetu.",
    "table1_headers": ["Povrch", "Ako typicky vzniká", "Čo vidíte", "Najväčšie riziko"],
    "table1_rows": [
        ("Ženilka", "Krátky vlas drží medzi nosnými priadzami jadra, ktoré sa ďalej tkajú alebo pletú.", "Mäkká chlpatá priadza s vlasom okolo osi.", "Úbytok vlasu, vytiahnutie celej priadze a zlepenie."),
        ("Buklé", "Efektná priadza tvorí nepravidelné slučky a uzlíky.", "Vystupujúce slučky, nie rovnomerný krátky vlas okolo jadra.", "Zachytenie a prerezanie konštrukčnej slučky."),
        ("Zamat", "Vlas vzniká ako súčasť tkanej vlasovej konštrukcie a následne sa reže.", "Súvislý smerový vlas na ploche.", "Otlačenie, lesk, mapa a citlivé profesionálne čistenie."),
        ("Plyš alebo fleece", "Vlas tvorí rezaná či česaná tkaná alebo pletená plocha.", "Povrch patrí nosnej látke, nie každej samostatnej efektnej priadzi.", "Žmolkovanie, zlepenie a tepelné poškodenie."),
        ("Obyčajný žmolok", "Krátke uvoľnené vlákna sa trením zamotajú na povrchu.", "Chumáč nepravidelne spojený s okolitými vláknami.", "Pri zámene môžete prerezať ženilkovú nosnú niť."),
    ],
    "sections": [
        {
            "heading": "Voľný chĺpok, žmolok, vytiahnutá priadza a lysá plocha",
            "paragraphs": [
                f"Voľný chĺpok sa môže zachytiť na oblečení bez viditeľnej zmeny tkaniny. Žmolok je zhluk povrchových vlákien; mechanizmus vysvetľuje článok <a href=\"{ARTICLE_PILLING}\">prečo sa textil žmolkuje</a>. Vytiahnutá ženilková priadza však pokračuje cez väzbu alebo očká a pri potiahnutí mení okolie. Lysá plocha ukazuje úbytok vlasu alebo opotrebované jadro.",
                "Povrch prezrite pri bočnom svetle a bez napínania. Voľný chumáč skúste nadvihnúť pinzetou iba natoľko, aby ste zistili, či sa hýbe okolie; neťahajte ho. Ak je spojený s dlhou niťou, nič neodstrihujte. Pri novom výrobku odfoťte hustotu vlasu na symetrickom mieste a kontaktujte predajcu pred holením alebo kefovaním.",
            ],
        },
        {
            "heading": "Prečo ženilka púšťa chĺpky a kedy to už nie je bežné",
            "paragraphs": [
                "Malé množstvo voľných koncov môže pochádzať z výroby, strihania, šitia alebo prvého používania. Rozsah však závisí od konštrukcie a nemá sa automaticky označiť za neškodný. Ak po každom jemnom dotyku pribúdajú chumáče, vznikajú viditeľné lysiny alebo sa uvoľňuje nosná priadza, ide o problém súdržnosti či nevhodné zaťaženie.",
                "Sledujte polohu: hrana sedadla, lakťová opierka, miesto pod popruhom a časté ohyby trpia viac než chránená plocha. Jednorazový chumáč po rozbalení má iný význam než rastúca línia po šve. Záznam fotografií pri rovnakom svetle je užitočnejší než silné vysávanie, ktoré odstráni dôkaz a môže samo urýchliť úbytok vlasu.",
            ],
        },
        {
            "heading": "Ako bezpečne odstrániť prach, chlpy a omrvinky",
            "paragraphs": [
                "Deku jemne vytraste a povrch prejdite hladkou mäkkou kefou v prirodzenom smere bez pritláčania. Pri čalúnení najprv nájdite pokyny výrobcu. Ak povoľuje vysávanie, zvoľte nízky výkon a hladkú hubicu cez ochrannú mriežku alebo sieťku. Rotačná kefa a úzka štrbina sa môžu prisať, zachytiť priadzu a vytvoriť lysú stopu.",
                "Lepiaci valček skúšajte na skrytom mieste. Príliš silné lepidlo vyťahuje povrchový vlas a zanecháva zvyšok, ktorý neskôr viaže prach. Zvieracie chlpy zdvíhajte po malých zónach bez trenia gumovým nástrojom, ak ho povoľuje výrobca poťahu. Na poškodenú niť nepoužívajte žiadny rotačný ani lepivý postup.",
            ],
        },
        {
            "heading": "Ako prať ženilkovú deku alebo prehoz",
            "paragraphs": [
                f"Najprv zistite, či ide o tkanú deku, pletený výrobok alebo poťah s podkladom. Všeobecný postup dopĺňa článok <a href=\"{ARTICLE_BLANKET}\">ako vyprať deku</a>. Ženilka navyše potrebuje ochranu pred zachytením: zabezpečte strapce podľa návodu, oddeľte zipsy a suché zipsy a nepreplňujte bubon. Veľký mokrý kus musí mať reálny priestor na pohyb.",
                "Použite len cyklus a teplotu zo štítku a presnú dávku kompatibilného prostriedku. Silná pena neznamená lepšie vyčistenie vlasu. Po praní deku vyberte s oporou, nekrúťte a nezdvíhajte za jeden roh. Pletený kus tvarujte naležato; tkaný sušte podľa etikety. Povrch upravujte až úplne suchý, jemne a bez agresívneho česania.",
            ],
        },
        {
            "heading": "Ako prať ženilkový sveter, šál alebo ručne vyrobený kus",
            "paragraphs": [
                "Páska priadze a etiketa hotového výrobku môžu uvádzať rozdielne obmedzenia; platí najnižší limit vrátane podšívky, gombíkov a pridaných materiálov. Ručne pletený kus nemusí mať stabilizované rozmery ako priemyselný výrobok. Pred prvým praním zmerajte dĺžku, šírku a rukávy na rovnej ploche a skontrolujte voľné konce priadze.",
                "Pri povolenom ručnom praní podoprite celý kus, vodu jemne pretláčajte a nešúchajte chlpaté plochy o seba. Pri strojovom cykle použite primerané ochranné vrecko len vtedy, ak nebráni oplachu. Mokré pletenie vyberte na podložke, prebytok vody odsajte bez krútenia a sušte naležato v rozmere. Vešiak môže predĺžiť ramená a stlačiť vlas.",
            ],
        },
        {
            "heading": "Ako čistiť ženilkovú sedačku a kreslo",
            "paragraphs": [
                "Najprv nájdite štítok nábytku, čistiaci kód a technický list poťahu. Zistite, či je látka snímateľná, či sa smie prať a či sa po zrazení dá bezpečne nasadiť. Zips na zadnej strane môže slúžiť iba montáži vo výrobe. Pod ženilkou môže byť podklad, lepidlo, pena, protipožiarna úprava a výplň, ktorú domáci návod na textil nezohľadňuje.",
                "Čerstvú tekutinu odsajte bielym savým materiálom bez drhnutia a bez zatláčania do peny. Lokálny výrobcom povolený produkt vyskúšajte na skrytom mieste a výsledok posúďte suchý. Nečistite jednu bodku tak mokro, že vznikne ostrá hranica. Veľká mastná škvrna, atrament, biologické znečistenie alebo premočenie výplne patria odbornému čisteniu čalúnenia.",
            ],
        },
        {
            "heading": "Škvrny od nápoja, jedla, make-upu a mastnoty",
            "paragraphs": [
                f"Najprv odstráňte prebytok bez rozotierania. Tekutinu odsajte, pevnú hmotu zdvihnite tupou hranou a mastnotu zachyťte savým materiálom. Rozdelenie postupov nájdete v článku <a href=\"{ARTICLE_STAIN}\">ako odstraňovať rôzne škvrny z textilu</a>. Ženilkový vlas nedrhnite krúživým pohybom; mení smer a môže sa vytrhnúť z jadra.",
                "Produkt nanášajte na handričku alebo presne podľa návodu výrobcu, nie priamo ako veľkú mláku. Postupujte od okraja a kontrolujte prenos farby. Pri čalúnení používajte minimum povolenej vlhkosti a rovnomerne odsávajte. Po vyschnutí porovnajte omak a lesk z viacerých uhlov; tmavšia zóna môže byť zvyšok, zlepený vlas, voda v podklade alebo zmena farby.",
            ],
        },
        {
            "heading": "Ako sušiť ženilku bez zlepenia a zatuchnutia",
            "paragraphs": [
                f"Mokrý kus rozložte s oporou a prúdením vzduchu podľa etikety. Rady pre domáce podmienky dopĺňa článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Ženilkový vlas nestláčajte pod štipcami, úzkou hranou ani mokrou vrstvou. Pravidelne kontrolujte rub, švy, strapce a hrubé preloženia, ktoré schnú dlhšie.",
                "Na sedačke smerujte bezpečné prúdenie vzduchu cez miestnosť, nie horúci fén na jednu mapu. Povrch môže byť suchý, kým pena pod ním ostáva vlhká. Poťah neuzatvárajte ani nezaťažujte sedením, kým sa nevráti rovnaká teplota a pach ako na suchej okolitej ploche. Ak vlhkosť prenikla hlboko, domáci ventilátor nenahrádza extrakciu a odborné posúdenie.",
            ],
        },
        {
            "heading": "Otlačený alebo zlepený vlas: čo možno skúsiť",
            "paragraphs": [
                "Najprv nechajte povrch úplne vyschnúť a zistite, či je vlas iba uložený jedným smerom, alebo chýba. Jemné urovnanie čistou rukou či výrobcom odporúčanou kefou môže zjednotiť odraz. Tvrdá kefa, holiaci strojček a agresívna para môžu odstrániť ďalší vlas. Vždy porovnajte skrytú skúšku po vychladnutí a vysušení.",
                "Zlepenie môže spôsobiť mastnota, nadbytok prostriedku, aviváž alebo teplo. Každá príčina potrebuje iný krok. Pri prateľnom kuse môže pomôcť povolený šetrný oplach bez ďalšej dávky; pri sedačke by ďalšia voda mohla zväčšiť mapu. Ak je syntetický vlas zatavený alebo jadro priadze poškodené, mechanické česanie pôvodnú štruktúru neobnoví.",
            ],
        },
        {
            "heading": "Ako opraviť vytiahnutú ženilkovú priadzu",
            "paragraphs": [
                f"Pri dlhom vytiahnutí povrch nenapínajte a niť neodstrihujte pri líci. Článok <a href=\"{ARTICLE_SNAGGING}\">o zatrhávaní textilu</a> vysvetľuje rozdiel medzi slučkou a pretrhnutím. Na tkanine môže odborník rozložiť napätie do susedných väzných bodov alebo vrátiť priadzu na rub. Pri pletenine treba zároveň zabezpečiť očko, aby sa nepáralo.",
                "Na čalúnení je oprava zaťažená sedením a podkladom. Lepidlo alebo uzol na líci vytvorí tvrdý hrbol a pri ďalšom trení reže okolie. Pri novej sedačke najprv využite reklamáciu a neodstraňujte dôkaz. Ak pazúr poškodil viac nití, obmedzte používanie miesta a zverte ho opravovni čalúnenia skôr, než sa otvor zväčší.",
            ],
        },
        {
            "heading": "Ako skladovať ženilkové deky, odevy a vankúše",
            "paragraphs": [
                "Ženilku ukladajte úplne čistú a suchú bez silného dlhodobého stlačenia. Pletenú deku zložte voľne, ťažký odev podoprite a vankúš nezasuňte pod ostrý kovový prvok. Povrch oddeľte od suchých zipsov, hrubých košov a predmetov, ktoré zachytávajú vlas. Tesný vákuový obal môže vytvoriť trvalé otlačenie a zadržať zvyškovú vlhkosť.",
                "Pred sezónnym uložením odstráňte omrvinky, pot a mastnotu, ktoré viažu prach. Vôňa ich neodstráni. Priedušný obal chráni pred prachom, no priestor musí zostať suchý. Po vybratí nechajte výrobok voľne ustáliť a povrch upravte iba jemne; okamžité horúce naparovanie stlačeného syntetického vlasu je zbytočné riziko.",
            ],
        },
        {
            "heading": "Ako vybrať ženilkový poťah alebo priadzu podľa použitia",
            "paragraphs": [
                f"Pri poťahu sa pýtajte na zloženie vlasu a jadra, nosnú väzbu, podklad, oder, žmolkovanie, posun priadzí, stálofarebnosť pri trení a čistiaci kód. Porovnanie slučkovej priadze ponúka článok <a href=\"{ARTICLE_BOUCLE}\">čo je buklé</a>. Jedno vysoké číslo oderu nehovorí, ako ľahko sa vlas zachytí alebo či sa dá škvrna lokálne čistiť.",
                "Pri klbku sledujte odporúčanú ihlu či háčik, skúšobnú hustotu, zloženie, pranie a spotrebu. Upleťte vzorku, zmerajte ju, operte podľa návodu a po vysušení posúďte úbytok vlasu, rozmer, omak a farbu. Pri výrobku pre dieťa alebo zviera zohľadnite uvoľňovanie vlákien a pevnosť švov; mäkkosť sama nie je dôkazom vhodnosti pre konkrétne použitie.",
            ],
        },
    ],
    "table2_heading": "Ženilka po používaní alebo čistení: čo môže znamenať zmena",
    "table2_intro": "Povrch sledujte z viacerých uhlov až suchý. Smer vlasu môže meniť odtieň bez toho, aby sa zmenilo farbivo.",
    "table2_headers": ["Prejav", "Možná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Tmavšia hladká plocha", "Uložený vlas, mastnota, zvyšok produktu alebo tepelné sploštenie.", "Omak, prenos, smer vlasu a stav po vysušení.", "Jemne urovnať iba pri povolení; nepridávať teplo naslepo."),
        ("Pribúdajú voľné chumáče", "Výrobný zvyšok, oder alebo uvoľňovanie vlasu z jadra.", "Či vznikajú lysiny a kde sa problém sústreďuje.", "Obmedziť trenie, zdokumentovať a pri raste reklamovať."),
        ("Dlhá chlpatá niť nad povrchom", "Vytiahnutá celá ženilková priadza.", "Pohyb okolia a pokračovanie na rube.", "Neodstrihovať; zveriť opravovni."),
        ("Tvrdý krúžok po škvrne", "Nerovnomerné zmáčanie, zvyšok produktu alebo presunutá mastnota.", "Rub, farbu a reakciu bielej handričky.", "Pri povolení rovnomerne odsať alebo opláchnuť."),
        ("Vlhký pach zo sedačky", "Voda prenikla do podkladu alebo peny.", "Hĺbku, trvanie a biologickú kontamináciu.", "Nepoužívať miesto a objednať odborné vysušenie či čistenie."),
    ],
    "steps_heading": "Ako ošetriť ženilkový výrobok krok za krokom",
    "steps": [
        "Určite zloženie vlasu a jadra, tkanú alebo pletenú konštrukciu, podklad, výplň a presný čistiaci kód.",
        "Pri bočnom svetle rozlíšte prach, cudzie vlákno, žmolok, vytiahnutú priadzu, lysinu a mapu.",
        "Voľné nečistoty odstráňte šetrne nasucho bez rotačnej kefy a bez priľnutia na poškodené miesto.",
        "Škvrnu odsajte a kompatibilný produkt vyskúšajte na skrytej zóne až do úplného vysušenia.",
        "Prateľnú deku či odev oddeľte od zipsov a perte iba povoleným cyklom s presnou dávkou.",
        "Neodnímateľné čalúnenie neponárajte a veľké premočenie zverte odbornému čisteniu.",
        "Mokrý kus podoprite, nekrúťte a sušte bez priameho tepla cez rub, švy, vlas aj výplň.",
        "Suchý povrch jemne urovnajte podľa výrobcu a rastúci úbytok vlasu alebo niť neopravujte holením.",
    ],
    "remember": [
        "Je povrch naozaj ženilková priadza, alebo buklé, zamat, plyš či obyčajný žmolok?",
        "Z akého vlákna je vlas, jadro, nosná látka, podklad, podšívka a šijacia niť?",
        "Je chumáč voľný, alebo pri pohybe ťahá celú priadzu a okolitú väzbu?",
        "Vzťahuje sa symbol na celý predmet, snímateľný poťah alebo iba jednu oddeliteľnú časť?",
        "Bude po mokrom zásahu možné úplne vysušiť rub, švy, podklad a výplň?",
        "Nezvyšuje zvolená kefa, zips, sušička alebo teplo úbytok vlasu?",
    ],
    "mistakes": [
        "Oholiť každú nerovnosť a prerezať nosnú alebo vytiahnutú ženilkovú priadzu.",
        "Vysávať rotačnou kefou alebo silným saním priamo cez uvoľnený vlas.",
        "Prať poťah sedačky iba preto, že má montážny zips.",
        "Premočiť škvrnu do peny a vysušiť iba viditeľný povrch.",
        "Predávkovať gél alebo aviváž a potom zlepený vlas agresívne česať.",
        "Použiť fén, radiátor alebo horúcu žehličku na otlačený syntetický povrch.",
    ],
    "expert_heading": "Odbornejší pohľad: jadro ženilkovej priadze, vlas a hodnotenie oderu",
    "expert": [
        "CottonWorks definuje ženilku ako vlasovú priadzu a odborný materiál o priadzach opisuje chumáčiky vlákien držané medzi skanými priadzami jadra. Táto geometria vysvetľuje veľkú mäkkú kontaktnú plochu aj riziko, že trenie odstráni vlas bez okamžitého pretrhnutia celej nosnej látky. Zloženie a parametre jadra však zostávajú výrobkovo špecifické.",
        "Recenzovaný výskum povrchových zmien pri Martindaleho odere uvádza predchádzajúce hodnotenie chenillového vlasu obrazovou analýzou a zdôrazňuje význam zákrutu, rovnomernosti priadze a povrchovej geometrie. ASTM D4966 meria oder za definovaných podmienok, no výsledok nie je priamou predpoveďou pazúra, zipsu alebo lokálneho mokrého drhnutia v domácnosti.",
        "ASTM D3939 sa zameriava na zachytávanie a D4034 na posun priadzí pri šve v čalúnnickej tkanine. Ide o odlišné poruchy než úbytok krátkeho vlasu. Pri porovnávaní poťahov preto žiadajte názov metódy, podmienky a konkrétnu skúšanú konštrukciu. Vysoký výsledok jedného testu nenahrádza čistiaci kód a skúšku farby.",
    ],
    "source_intro": "Zdroje podporujú opis ženilkovej priadze, význam jej jadra a vlasu aj rozdiel medzi oderom, zachytením a posunom priadzí. Nepodporujú univerzálne mokré čistenie všetkých ženilkových poťahov.",
    "sources": [
        ("CottonWorks: definícia ženilkovej priadze", COTTONWORKS_CHENILLE),
        ("CottonWorks: odborný materiál o efektných priadzach", COTTONWORKS_YARNS),
        ("Recenzovaná štúdia: povrchové zmeny pri Martindaleho odere", PMC_SURFACE_ABRASION),
        ("Recenzovaná štúdia: konštrukcia ženilkovej integrovanej priadze", PMC_CHENILLE),
        ("ASTM D4966: oder textílií metódou Martindale", ASTM_ABRASION),
        ("ASTM D3939: odolnosť proti zachyteniu", ASTM_SNAG),
        ("ASTM D4034: posun priadzí pri šve čalúnnických tkanín", ASTM_UPHOLSTERY_SLIP),
        ("AATCC TM61: stálofarebnosť pri praní", AATCC_COLOR),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je buklé a ako ho čistiť", ARTICLE_BOUCLE),
        ("Čo je zamat a ako sa oň starať", ARTICLE_VELVET),
        ("Prečo sa textil žmolkuje", ARTICLE_PILLING),
        ("Ako predchádzať zatrhávaniu", ARTICLE_SNAGGING),
        ("Ako vyprať deku", ARTICLE_BLANKET),
        ("Ako odstrániť rôzne škvrny", ARTICLE_STAIN),
    ],
    "faq_title": "ženilka, chenille priadza a ženilkové poťahy",
    "faq": [
        ("Čo je ženilka?", "Je to efektná priadza s krátkym mäkkým vlasom zachyteným medzi nosnými niťami jadra."),
        ("Je chenille to isté ako ženilka?", "Áno, chenille je medzinárodné a francúzske označenie; v slovenčine sa bežne používa ženilka."),
        ("Je ženilka vždy polyesterová?", "Nie. Vlas aj jadro môžu byť z bavlny, viskózy, polyesteru, akrylu alebo zmesi."),
        ("Prečo ženilka púšťa chĺpky?", "Môže ísť o výrobný zvyšok alebo oder, ale rastúce lysiny a vyťahovanie nosnej priadze už treba riešiť."),
        ("Môžem ženilku oholiť od žmolkov?", "Nie naslepo. Holiaci strojček môže prerezať vytiahnutú alebo nosnú priadzu, ktorú ste zamenili za žmolok."),
        ("Môže sa ženilková deka prať v práčke?", "Len ak to povoľuje etiketa konkrétnej deky a zariadenie má dostatočnú kapacitu na mokrý kus."),
        ("Na koľko stupňov prať ženilku?", "Jedna teplota neexistuje. Rozhoduje zloženie vlasu a jadra, nosná konštrukcia, farba a symbol."),
        ("Môžem vyprať poťah zo ženilkovej sedačky?", "Iba ak to výrobca výslovne povoľuje. Zips sám osebe nie je dôkaz prateľnosti."),
        ("Ako odstrániť škvrnu zo ženilkovej sedačky?", "Tekutinu odsajte bez trenia a použite iba výrobcom povolený postup po skrytej skúške; penu nepremočte."),
        ("Môže ísť ženilka do sušičky?", "Len pri výslovnom symbole. Teplo a prevaľovanie môžu zlepiť, zataviť alebo vytrhnúť vlas."),
        ("Dá sa otlačený vlas napariť?", "Niekedy iba pri výslovnom povolení a po skrytej skúške. Para môže poškodiť syntetiku, viskózu, farbu aj podklad."),
        ("Čo robiť s vytiahnutou chlpatou niťou?", "Neodstrihujte ju. Položte povrch bez napätia, skontrolujte rub a zverte opravu odborníkovi."),
        ("Ako ženilku skladovať?", "Úplne čistú a suchú, voľne, bez vákuového stlačenia a mimo predmetov, ktoré zachytávajú vlas."),
    ],
}

commercial(
    CHENILLE,
    noun="ženilkový odev alebo poťah",
    limit=(
        "Gél použite iba na prateľnú deku, odev alebo výrobcom povolený snímateľný poťah. "
        "Nie je automaticky vhodný na neodnímateľnú sedačku, viskózový vlas, lepený podklad "
        "ani profesionálne čistiteľný kus a neobnoví vytrhnutý vlas."
    ),
)


PANAMA: dict[str, object] = {
    "title": "Čo je panamová väzba: košíková tkanina, posun nití a pranie",
    "link": "co-je-panamova-vazba-kosikova-tkanina-posun-niti-a-pranie",
    "meta": "Čo je panamová alebo košíková väzba, ako sa líši od plátna a Oxfordu a ako prať a chrániť tkaninu pred posunom nití, zatrhnutím a deformáciou švov.",
    "short": "Panamová alebo košíková väzba je odvodenina plátnovej väzby, pri ktorej dve či viaceré osnovné a útkové nite pracujú v skupinách. Povrch je plastickejší, no pri voľnej konštrukcii sa nite ľahšie posúvajú.",
    "name": "panamová väzba",
    "locative": "panamovej väzbe",
    "identity_heading": "Panamová väzba zoskupuje nite do košíkového rytmu",
    "identity_detail": "Namiesto pravidla jedna niť nad jednou pod sa dve alebo viaceré osnovné nite správajú ako skupina a križujú zodpovedajúcu skupinu útku, napríklad v opakovaní 2 × 2.",
    "identity_boundary": "Košíkovú väzbu možno utkať z bavlny, ľanu, vlny, polyesteru aj zmesi a obchodné slová panama či hopsack môžu označovať odlišne husté a hrubé výrobky.",
    "label_focus": "presné vlákna, veľkosť skupín, hustotu, zákrut priadze, elastan, podšívku, švy, okraje, povrchovú úpravu, potlač, určenie výrobku a povolenú teplotu a odstreďovanie",
    "missing_label": "Pri metráži vyžiadajte technický list a skúšku rozmerovej zmeny; pri hotovom odeve bez etikety neodvodzujte program z viditeľných štvorčekov alebo zo slova hopsack.",
    "dry_check": "posunuté skupiny nití, svetlé pruhy pri šve, rozšírené otvory, vytiahnuté slučky, oder vrcholov, skrútenie okraja, nepravidelný raster, staré mapy a oslabené stehy",
    "damage_boundary": "Prach a škvrnu možno čistiť, no skupina nití posunutá pri šve, prerezaná priadza alebo trvalo skosený diel sa ďalším praním mechanicky neopraví.",
    "test_focus": "Skúšobnú zónu sledujte v osnovnom aj útkovom smere a proti svetlu, pretože rovnaký tlak môže presunúť jednu sústavu viac než druhú bez okamžitého pretrhnutia.",
    "combined_risk": "napučania zoskupených priadzí, menšieho počtu väzných bodov na jednotlivú niť, povrchového trenia a ťahu, ktorý pri šve rozdeľuje celé skupiny",
    "chemistry_boundary": "Nadbytok gélu sa môže držať v medzerách a pri hrubších priadzach, ale silnejšia mechanika na jeho odstránenie zvyšuje posun a oder; dávku a oplach treba riešiť spolu.",
    "drying_detail": "Odev podoprite podľa švov, obrus rozložte po šírke a poťah vetrajte aj z rubu; otvorenejšia plocha môže na povrchu schnúť rýchlo, kým hrubé priadze a lemy ostávajú vlhké.",
    "heat_boundary": "Vysoké teplo môže rozdielne zraziť skupiny, poškodiť syntetickú zložku, ustáliť skosenie, vytvoriť lesk na vrcholoch a deformovať podšívku alebo výstuž.",
    "stop_signs": "rastúci posun pri šve, otváranie rastra, púšťanie farby, lepkavosť úpravy, prasknutie priadze, zvlnenie podšívky, uvoľnenie záteru alebo zväčšujúca sa mapa",
    "professional_boundary": "Bežnú prateľnú bavlnenú panamu možno často ošetrovať doma podľa etikety, zatiaľ čo vlnený hopsack, podšité sako, čalúnenie, lepená taška alebo historický textil potrebuje výrobcom určený či odborný postup.",
    "answer": "Panamová, košíková alebo basket väzba je odvodenina plátna, pri ktorej sa dve či viaceré nite správajú ako skupina a prechádzajú nad a pod skupinou v druhom smere. Vzniká zreteľnejší štvorcový raster, mäkší ohyb a často lepšia splývavosť než pri jednoduchom plátne, ale aj menšia stabilita pri posune nití. Hopsack sa v predaji používa pre otvorenejšie košíkové tkaniny a nepatrí mu samostatný univerzálny program. Pred praním skontrolujte zloženie, hustotu, švy a celý výrobok. Prateľný kus oddeľte od háčikov, nepreplňujte bubon, zvoľte symbolom povolený cyklus a mokrý textil nenaťahujte do šírky. Svetlú líniu pri šve riešte ako konštrukčný problém, nie škvrnu na drhnutie.",
    "intro": "Košíkový povrch pôsobí pevne, pretože vidíme hrubšie bloky priadzí. Zoskupenie však zároveň znižuje počet miest, na ktorých je každá jednotlivá niť pevne previazaná s druhým smerom. Voľnejšia panamová tkanina sa preto môže pri tesnom šve, opakovanom sedení alebo tvrdom kefovaní posunúť bez toho, aby priadza hneď praskla. Pod jedným názvom sa navyše predáva ľahká košeľovina, obrus, vyšívacie plátno, vlnené sako aj poťah. Praktická starostlivosť musí oddeliť väzbu od vlákna, hustoty, šitia a funkcie hotového výrobku.",
    "quick": [
        "<strong>Skupiny nití tvoria raster:</strong> bežná konštrukcia 2 × 2 pracuje s dvoma osnovnými a dvoma útkovými niťami ako blokmi.",
        "<strong>Je to odvodenina plátna:</strong> väčšie väzné bloky dávajú textúru a splývavosť, ale znižujú stabilitu oproti jednoduchému 1 × 1.",
        "<strong>Hopsack patrí do rovnakej rodiny:</strong> názov často označuje otvorenú košíkovú tkaninu, nie nové vlákno.",
        "<strong>Svetlá línia pri šve môže byť posun:</strong> nite sú celé, ale odtlačili sa od stehu; drhnutie problém zhoršuje.",
        "<strong>Chráňte otvorený povrch:</strong> zipsy, suchý zips, prstene a pazúry ľahšie zachytia dlhší väzný úsek.",
        "<strong>Pranie určuje celý predmet:</strong> bavlnený obrus, vlnené sako a čalúnnický poťah nepatria na jeden program.",
    ],
    "overview_heading": "Ako funguje košíková väzba a prečo je plastickejšia",
    "overview": [
        "CottonWorks opisuje basket weave ako odvodeninu plátnovej väzby, pri ktorej skupiny dvoch alebo viacerých osnovných nití prechádzajú nad a pod skupinami útku. Najbežnejší pravidelný raster 2 × 2 sa opakuje na štyroch osnovných a štyroch útkových pozíciách. Dve nite sa pritom vizuálne a mechanicky správajú ako širší pás, takže povrch pripomína prepletaný kôš.",
        "V porovnaní s jednoduchým plátnom má košíková väzba menej väzných bodov na jednotlivú niť. CottonWorks preto uvádza viac textúry a splývavosti, ale menšiu stabilitu. Konkrétny výsledok mení hustota, zákrut, hrúbka a trenie priadzí. Kompaktná jemná košeľovina a otvorený vlnený hopsack môžu mať príbuznú geometriu a pritom úplne odlišný posun, omak aj údržbu.",
        "Nepravidelné košíkové väzby nemusia mať rovnaký počet nití v oboch skupinách. Oxford sa bežne opisuje ako 2 × 1 odvodenina používaná na košele. Panama 2 × 2, vyšívacie plátno a čalúnnický basket môžu pracovať s inou hustotou a určením. Názov preto nehovorí, či sú medzery funkčné, či je povrch záterovaný alebo či šev potrebuje špeciálne spevnenie.",
    ],
    "table1_heading": "Plátno, panama, Oxford, canvas a hopsack",
    "table1_intro": "Všetky názvy môžu používať príbuzný pravouhlý raster, ale skupiny nití, hustota, hmotnosť a určenie sa líšia.",
    "table1_headers": ["Označenie", "Typické previazanie", "Povrch a použitie", "Praktická hranica"],
    "table1_rows": [
        ("Jednoduché plátno", "Jedna osnova nad jedným útkom v pravidelnom rytme 1 × 1.", "Rovnomerný stabilnejší raster v širokom rozsahu hmotností.", "Stále závisí od priadze, hustoty, farby a dokončenia."),
        ("Panama alebo basket", "Dve či viac nití nad a pod zodpovedajúcou skupinou, často 2 × 2.", "Plastickejší štvorcový raster, odevy, obrusy, dekor a poťahy.", "Menšia stabilita, posun skupín a zachytenie."),
        ("Oxford", "Často nepravidelná košíková väzba 2 × 1 s jemnejšou osnovou a hrubším útkom.", "Košeľová tkanina s mäkkou zrnitou plochou.", "Golier, manžety, rozmer a konkrétne farbenie."),
        ("Canvas", "Najčastejšie husté plátno alebo príbuzná pevná konštrukcia.", "Ťažšie tašky, obuv, pracovné a dekoračné výrobky.", "Zátery, lepidlá a vysoká mokrá hmotnosť."),
        ("Hopsack", "Obchodné označenie otvorenejšej košíkovej väzby, často pri sakách a čalúnení.", "Vzdušný zreteľný raster a väčšie medzery.", "Zachytenie, posun pri šve a vlnené či podšité výrobky."),
    ],
    "sections": [
        {
            "heading": "Ako rozoznať panamovú väzbu od obyčajného plátna",
            "paragraphs": [
                "Položte látku na kontrastný podklad a lupou sledujte jeden úplný opakovací blok. Pri plátne sa každá osnova strieda nad a pod každým útkom. Pri pravidelnej paname dve susedné osnovy prejdú spoločne nad dvoma útkami a potom pod ďalšími dvoma. Hrubá viacnásobná priadza môže vzhľad skupiny napodobniť, preto sledujte jednotlivé komponenty, nie iba šírku pásu.",
                "Pozrite aj rub a okraj. Skutočná väzba pokračuje cez plochu, kým reliéfna potlač, razba alebo záter môže byť hlavne na líci. Domáce rozpoznanie neodhaľuje presné vlákno, pevnosť ani povrchovú úpravu. Pri hotovom výrobku preto stále potrebujete etiketu a pri metráži technický list s hustotou, hmotnosťou a odporúčanou údržbou.",
            ],
        },
        {
            "heading": "Panamová väzba verzus Oxford a názov hopsack",
            "paragraphs": [
                "Oxford je príbuzná nepravidelná košíková väzba, pri ktorej dvojica osnovných nití typicky pracuje cez jednotlivé útkové nite. Panama sa často používa pre pravidelné 2 × 2 alebo väčšie bloky. V predaji však hranice nie sú dôsledné a obchodný názov môže označiť vzhľad, hmotnosť či určenie namiesto presného väzbového zápisu.",
                "Hopsack nie je samostatné vlákno. Zvyčajne označuje otvorenú košíkovú tkaninu s čitateľným rastrom, často na letné saká, kabátiky, dekor alebo čalúnenie. Preto patrí do tohto článku ako rovnaká konštrukčná rodina. Vlnený podšitý hopsack sa však nesmie prať podľa bavlneného obrusu; presné zloženie a stavba výrobku majú vždy prednosť.",
            ],
        },
        {
            "heading": "Prečo sa nite pri šve posúvajú bez pretrhnutia",
            "paragraphs": [
                "Pri napätí kolmom na šev sa skupiny osnovy alebo útku môžu odtlačiť od stehu. Objaví sa svetlejšia línia alebo otvorené medzery, hoci samotné priadze zostávajú celé. Voľnejšia väzba, hladké vlákno, tesný strih, malá švová rezerva, nevhodná hustota stehu a opakované sedenie sa môžu sčítať.",
                "Rozlíšenie od trhliny je dôležité. Pri trhline vidíte prerušené vlákna; pri posune sa raster zhustí na jednej strane a otvorí na druhej. Odev ďalej nenapínajte a miesto nedrhnite. Krajčír môže posúdiť rezervu, výstuž a typ šva. Pranie ani kvapka lepidla na líci nevrátia správne rozloženie zaťaženia.",
            ],
        },
        {
            "heading": "Ako prať bavlnenú panamu, obrus alebo ľahký odev",
            "paragraphs": [
                f"Najprv prečítajte <a href=\"{ARTICLE_LABEL}\">materiálový a ošetrovací štítok</a>. Vyprázdnite vrecká, zabezpečte kovanie, skontrolujte posunuté nite a oddelte zipsy, háčiky a suchý zips. Pri obruse odstráňte omrvinky a označte škvrny. Ak je pranie povolené, použite podobne ľahkú a farebne kompatibilnú náplň s dostatkom priestoru.",
                "Zvoľte iba teplotu a mechaniku zo symbolu. Otvorenejšia väzba nepotrebuje silnejší program; práve naopak, prudké trenie zvyšuje posun a zachytenie. Prostriedok dávkujte podľa vody, hmotnosti a znečistenia a nenalievajte ho priamo na suchý raster. Po cykle kus podoprite a urovnajte švy bez roztiahnutia otvorov.",
            ],
        },
        {
            "heading": "Vlnené hopsack sako a podšitý výrobok",
            "paragraphs": [
                "Sako obsahuje vrchnú tkaninu, výstuž, podšívku, ramenné prvky, nite, gombíky a tvarovanie. Aj keď otvorená vlnená väzba pôsobí vzdušne, kombinácia vrstiev môže povoľovať iba profesionálne čistenie. Domáci program pre vlnu nie je automaticky rovnocenný a mokré sako môže stratiť tvar, zvlniť výstuž alebo zmeniť dĺžku podšívky.",
                "Medzi čisteniami sako vetrajte na širokom vešiaku, jemne odstráňte povrchový prach a čerstvú škvrnu odsajte bez tlaku. Vytiahnutú niť neholte. Ak zmoklo, podoprite ho a nechajte vyschnúť mimo radiátora. Zmenu pri šve, lokálny lesk alebo opakovaný pach riešte podľa etikety s odbornou čistiarňou, nie silnejšou vôňou.",
            ],
        },
        {
            "heading": "Panama na taške, topánke a čalúnení nie je obyčajná bielizeň",
            "paragraphs": [
                f"Taška môže mať lepenku, penu, kožu, kovanie a záter; obuv lepidlo a tvarovanú podošvu; čalúnenie podklad, penu a rám. Aj hustá tkanina preto nemusí byť ponoriteľná. Porovnanie pevného plátna dopĺňa článok <a href=\"{ARTICLE_CANVAS}\">čo je canvas a ako ho čistiť</a>. Najprv hľadajte návod celého výrobku.",
                "Prach odsajte povoleným nízkym výkonom a škvrnu riešte lokálne po skrytej skúške. Neotvárajte väzbu tvrdou kefou a nepreháňajte vodu do výplne. Snímateľný poťah perte iba pri výslovnom povolení; zips môže slúžiť montáži. Pri veľkej mastnote, neznámom farbive alebo biologickom premočení zvoľte odborné čistenie vhodné pre celý systém.",
            ],
        },
        {
            "heading": "Ako odstrániť škvrnu bez roztiahnutia rastra",
            "paragraphs": [
                f"Čerstvú tekutinu odsajte z líca a dostupného rubu bez krúživého drhnutia. Pevnú nečistotu zdvihnite tupou hranou. Podľa povahy škvrny postupujte podľa návodu <a href=\"{ARTICLE_STAIN}\">ako odstraňovať rôzne škvrny z textilu</a>. Tvrdá kefa sa zachytáva o dlhšie väzné úseky a rozťahuje skupiny, najmä na otvorenom hopsacku.",
                "Kompatibilný produkt vyskúšajte na skrytom mieste a po úplnom vyschnutí sledujte farbu aj šírku otvorov. Pracujte od okraja ku stredu a prostriedok rovnomerne odsajte alebo opláchnite spôsobom povoleným výrobcom. Svetlejšia mapa môže byť zvyšok, strata farby alebo zmena geometrie; opakované trenie naslepo všetky tri možnosti zhoršuje.",
            ],
        },
        {
            "heading": "Zachytenie o zips, prsteň, pazúr a suchý zips",
            "paragraphs": [
                f"Skupinová väzba vytvára väčšie úseky, pod ktoré sa ľahšie zachytí háčik. Prevenciu rozoberá článok <a href=\"{ARTICLE_SNAGGING}\">o zatrhávaní textilu</a>. Pri praní zatvorte alebo oddeľte ostré kovanie a suchý zips, no nevytvorte napätie na odeve. Pri používaní sledujte hrany stola, popruhy a zvieracie pazúry.",
                "Ak sa vytiahla slučka, tkaninu položte bez napätia a neťahajte koniec. Jemným rozložením napätia možno priadzu vrátiť do susedných väzných bodov, ale otvorený alebo hodnotný výrobok zverte odborníkovi. Odstrihnutie vytvorí dva voľné konce a môže oslabiť celý blok. Pri novom kuse najprv zdokumentujte rozsah pre reklamáciu.",
            ],
        },
        {
            "heading": "Sušenie bez skosenia, vytiahnutia a tvrdých máp",
            "paragraphs": [
                f"Mokrý kus podoprite a urovnajte do prirodzeného tvaru bez naťahovania skupín. Všeobecné zásady ponúka článok <a href=\"{ARTICLE_DRYING}\">ako sušiť bielizeň bez zatuchnutia</a>. Odev nevešajte za úzky bod a obrus nepricviknite cez poškodený raster. Otvorenejšia plocha schne rýchlo, ale hrubé priadze, lemy a podšívka potrebujú viac času.",
                "Sušičku použite iba pri povolenom symbole. Prevaľovanie môže zvýšiť povrchový oder a teplo ustáliť skosenie alebo zrazenie. Pri zavesení rozložte hmotnosť po šírke a chráňte farbu pred priamym slnkom. Pred meraním nechajte textil úplne vyschnúť a ustáliť; mokrú odchýlku nenaprávajte silným ťahom.",
            ],
        },
        {
            "heading": "Ako žehliť košíkovú tkaninu bez lesku a deformácie",
            "paragraphs": [
                f"Žehlenie povoľuje symbol a najcitlivejšia zložka. Použite ochrannú tkaninu, pracujte z rubu a s malým tlakom, aby sa vrcholy rastra nesploštili do lesklých štvorcov. Praktické základy dopĺňa článok <a href=\"{ARTICLE_IRONING}\">ako vyžehliť košeľu</a>. Otvorený vlnený hopsack a podšité sako radšej zverte odbornému tvarovaniu.",
                "Žehličkou neposúvajte mokré skupiny do strán. Para môže uvoľniť záhyb, ale zároveň napučať priadze a zmeniť rozmer alebo lepenú výstuž. Skrytý test nechajte vychladnúť a skontrolujte z boku aj proti svetlu. Lesklý sploštený raster alebo poškodená syntetická niť sa vyššou teplotou nevrátia.",
            ],
        },
        {
            "heading": "Ako skladovať panamové odevy, obrusy a metráž",
            "paragraphs": [
                "Výrobok uložte čistý, suchý a bez napätia na poškodený šev. Sako zaveste na širokú oporu, obrus zložte vo veľkých prehyboch a metráž podľa hmotnosti voľne naviňte alebo podoprite. Otvorený raster nedávajte priamo k suchému zipsu, prútenému košu či ostrému kovaniu. Tesné vákuové stlačenie môže vytvoriť trvalý lom.",
                "Pred sezónnym uložením odstráňte mastnotu a omrvinky, ktoré priťahujú prach. Vôňa nenahrádza čistenie ani sušenie. Pri vlnenom hopsacku dodržte ochranu pred škodcami podľa bezpečného návodu a pravidelne kontrolujte skryté záhyby. Dlhý tlak na jednu hranu môže raster skosiť, preto polohu občas zmeňte.",
            ],
        },
        {
            "heading": "Ako vybrať panamovú tkaninu podľa určenia",
            "paragraphs": [
                f"Pri odeve sledujte zloženie, hustotu, hmotnosť, splývavosť, posun pri šve, zachytenie a rozmerovú zmenu. Pri obruse pridajte stálofarebnosť a škvrny, pri poťahu oder a čistiaci kód. Porovnanie hustej posteľnej tkaniny ponúka článok <a href=\"{ARTICLE_PERCALE}\">čo je perkál</a>. Rovnaký raster neznamená rovnakú funkciu.",
                "Vyžiadajte si názov skúšobnej metódy a podmienky, nie iba číslo bez jednotky. Vzorku ohnite, pozorujte medzery a jemne zaťažte okraj bez poškodenia. Pri metráži ušite skúšobný šev, predperte ho plánovaným spôsobom a po vysušení porovnajte posun, rozmery a farbu. Kvalitný výber spája stabilitu s požadovanou vzdušnosťou a opraviteľnosťou.",
            ],
        },
    ],
    "table2_heading": "Panamová väzba po praní alebo používaní: diagnostická tabuľka",
    "table2_intro": "Raster pozorujte bez napätia na kontrastnom podklade. Posun, trhlina, škvrna a lesk potrebujú odlišné riešenie.",
    "table2_headers": ["Prejav", "Pravdepodobná príčina", "Čo overiť", "Bezpečný ďalší krok"],
    "table2_rows": [
        ("Svetlá línia pri šve", "Posun osnovy alebo útku od stehu.", "Či sú priadze celé a raster zhustený vedľa otvoru.", "Prestať zaťažovať a riešiť šev krajčírom."),
        ("Dlhá slučka nad povrchom", "Zachytená skupina alebo jednotlivá priadza.", "Pokračovanie na rube a pohyb okolia.", "Neodstrihovať; rozložiť napätie alebo odborne opraviť."),
        ("Raster je šikmý", "Skosenie pri výrobe, praní alebo sušení pod ťahom.", "Smer osnovy, útku, švov a suché rozmery.", "Nenapínať mokrý; pri novom kuse zdokumentovať."),
        ("Tvrdé tmavé body", "Zvyšok gélu, mastnota alebo minerály medzi skupinami.", "Prenos na bielu handričku a oplach.", "Pri povolení šetrne opláchnuť bez ďalšej dávky."),
        ("Lesklé sploštené štvorce", "Tlak, trenie alebo príliš vysoké teplo.", "Zmenu omaku a zloženie vlákna.", "Nezohrievať znova; obmedziť ďalší oder."),
    ],
    "steps_heading": "Ako vyprať prateľnú panamovú tkaninu krok za krokom",
    "steps": [
        "Určite väzbu, zloženie, hustotu, podšívku, výstuž, záter a všetky symboly starostlivosti.",
        "Na kontrastnom podklade skontrolujte posun pri šve, vytiahnuté skupiny, trhliny a staré mapy.",
        "Prach odstráňte nasucho a škvrnu odsajte; kompatibilný produkt vyskúšajte v oboch smeroch.",
        "Oddeľte tkaninu od zipsov, háčikov, suchého zipsu a ťažkých textílií, ktoré raster zachytávajú.",
        "Použite iba povolený cyklus, primeranú náplň a presnú dávku s dostatkom vody na oplach.",
        "Mokrý kus podoprite, nekrúťte a neposúvajte skupiny naťahovaním šva alebo otvorov.",
        "Sušte podľa etikety s rozloženou hmotnosťou a kontrolujte hrubé priadze, lem aj podšívku.",
        "Žehlite iba pri povolení z rubu s nízkym tlakom a po vychladnutí porovnajte raster a rozmery.",
    ],
    "remember": [
        "Ide o pravidelnú panamu, Oxford, husté plátno alebo otvorený hopsack?",
        "Aké vlákno, hustota, zákrut, podšívka, výstuž a záter určujú najnižší limit?",
        "Je svetlá línia škvrna, posun celej skupiny, trhlina alebo zmena odrazu?",
        "Sú zipsy, háčiky a suchý zips oddelené od otvorenejšieho rastra?",
        "Má náplň priestor na pohyb a oplach bez naťahovania pri šve?",
        "Je výrobok suchý aj v hrubých skupinách, leme, švoch a podšívke?",
    ],
    "mistakes": [
        "Považovať hrubší košíkový raster za automaticky pevnejší než jednoduché plátno.",
        "Vytvoriť samostatný postup pre hopsack a ignorovať, že ide o košíkovú rodinu s rôznym zložením.",
        "Drhnúť svetlú líniu pri šve ako škvrnu a ďalej posúvať neporušené nite.",
        "Prať otvorenú väzbu so zipsami, háčikmi a suchým zipsom bez ochrany.",
        "Napínať mokrý raster do šírky alebo ho sušiť za dva úzke body.",
        "Žehliť vrcholy vysokým tlakom, kým vzniknú lesklé sploštené štvorce.",
    ],
    "expert_heading": "Odbornejší pohľad: väzné body, deformácia priadzí a švová integrita",
    "expert": [
        "CottonWorks opisuje pravidelnú 2 × 2 košíkovú väzbu ako derivát plátna, v ktorom dve osnovy pracujú cez dva útky ako jeden blok. Uvádza viac textúry a splývavosti, ale menšiu stabilitu než pri jednoduchom plátne. Tento všeobecný vzťah nemení potrebu hodnotiť konkrétnu hustotu, zákrut, trenie a dokončenie priadzí.",
        "ASTM D1336 meria deformáciu jednej sústavy nití cez druhú po povrchovom trení a uvádza osobitnú vhodnosť pre otvorené väzby. ASTM D1683 sa venuje zlyhaniu šitého šva a rozlišuje pretrhnutie, posun aj integritu zostavy. ASTM D4034 je špecializovaný na posun pri šve v čalúnnických tkaninách. Výsledky sa nedajú zamieňať ani preniesť bez podmienok.",
        "AATCC TM135 sleduje rozmerovú zmenu po definovanom praní, kým TM61 stálofarebnosť a povrchové zmeny pri zrýchlenom postupe. Otvorený raster môže zmeniť rozmery a farbu rozdielne v osnove a útku. Pre spotrebiteľa je užitočný technický list s metódou, smerom a konkrétnym výrobkom; obchodný názov panama alebo hopsack takú informáciu nenahrádza.",
    ],
    "source_intro": "Zdroje podporujú košíkovú väzbu ako odvodeninu plátna, nižšiu stabilitu a rozdiel medzi deformáciou, švovým posunom, trhaním a rozmerovou zmenou. Nepodporujú jeden cyklus pre všetky výrobky z tejto väzby.",
    "sources": [
        ("CottonWorks: košíková a Oxford väzba", COTTONWORKS_BASIC_WEAVES),
        ("CottonWorks: odborný prehľad tkania", COTTONWORKS_WEAVING),
        ("CottonWorks: kvalita konštrukcie priadzí a tkanín", COTTONWORKS_QUALITY),
        ("ASTM D1336: deformácia priadzí v tkanine", ASTM_DISTORTION),
        ("ASTM D1683: zlyhanie šitých švov", ASTM_SEAM),
        ("ASTM D4034: posun priadzí pri šve čalúnnických tkanín", ASTM_UPHOLSTERY_SLIP),
        ("ASTM D3939: odolnosť proti zachyteniu", ASTM_SNAG),
        ("AATCC TM135: rozmerové zmeny po praní", AATCC_DIMENSION),
        ("EÚ 1007/2011: označovanie textilných vlákien", EU_FIBRE_LABEL),
        ("GINETEX: význam symbolov ošetrovania", GINETEX),
    ],
    "related": [
        ("Čo je canvas a ako ho prať", ARTICLE_CANVAS),
        ("Čo je perkál a čo znamená hustota", ARTICLE_PERCALE),
        ("Ako čítať štítok na oblečení", ARTICLE_LABEL),
        ("Ako predchádzať zatrhávaniu", ARTICLE_SNAGGING),
        ("Prečo sa textil po praní zráža", ARTICLE_SHRINKAGE),
        ("Ako farby blednú pri praní a trení", ARTICLE_COLOR),
    ],
    "faq_title": "panamová, košíková a hopsack väzba",
    "faq": [
        ("Čo je panamová väzba?", "Je to odvodenina plátna, pri ktorej dve či viaceré osnovné a útkové nite pracujú v skupinách."),
        ("Je panama to isté ako košíková väzba?", "V textilnom kontexte sa panama bežne používa pre pravidelnú košíkovú alebo basket väzbu, napríklad 2 × 2."),
        ("Čo znamená hopsack?", "Zvyčajne ide o obchodné označenie otvorenejšej košíkovej tkaniny, často na saká alebo čalúnenie."),
        ("Je Oxford panamová väzba?", "Je to príbuzná nepravidelná košíková odvodenina, často 2 × 1, používaná najmä na košele."),
        ("Je panamová tkanina vždy bavlnená?", "Nie. Môže byť z bavlny, ľanu, vlny, polyesteru alebo zmesi."),
        ("Môže sa panamová tkanina prať v práčke?", "Len ak to povoľuje etiketa celého výrobku vrátane podšívky, výstuže, záteru a ozdôb."),
        ("Na koľko stupňov prať panamu?", "Jedna teplota neexistuje. Rozhoduje zloženie, farba, hustota, dokončenie a symbol konkrétneho kusu."),
        ("Prečo sa pri šve objavila svetlá línia?", "Priadze sa mohli odtlačiť od stehu bez pretrhnutia. Miesto nedrhnite a nechajte šev posúdiť."),
        ("Ako opraviť vytiahnutú niť?", "Neodstrihujte ju. Rozložte tkaninu bez napätia a pri viditeľnom alebo zaťaženom mieste využite odbornú opravu."),
        ("Môže ísť panama do sušičky?", "Len pri výslovnom symbole. Teplo a prevaľovanie môžu meniť rozmer, raster, farbu aj podšívku."),
        ("Ako žehliť košíkovú tkaninu?", "Podľa symbolu, z rubu, cez ochrannú tkaninu a s nízkym tlakom, aby sa raster nesploštil."),
        ("Je otvorenejší hopsack odolnejší?", "Nie automaticky. Väčšie medzery zvyšujú vzdušnosť, ale môžu znížiť stabilitu a zvýšiť zachytávanie."),
        ("Ako vybrať panamovú metráž?", "Porovnajte zloženie, hustotu, hmotnosť, posun pri šve, zachytenie, rozmerovú zmenu a určenie; potom operte skúšobný šev."),
    ],
}

commercial(
    PANAMA,
    noun="výrobok z panamovej väzby",
    limit=(
        "Gél nie je automaticky vhodný na vlnený hopsack, podšité sako, lepenú tašku, "
        "čalúnenie ani výrobok určený na profesionálne čistenie. Neopraví posunuté skupiny "
        "nití, otvorený šev alebo teplom sploštený raster."
    ),
)


ARTICLES: list[dict[str, object]] = [CHAMBRAY, MATELASSE, CHENILLE, PANAMA]


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
        "batch": "batch-51",
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "target_count": len(target_urls),
        "outgoing_count": len(outgoing_urls),
        "check_count": len(checks),
        "failure_count": sum(not check["ok"] for check in checks),
        "checks": checks,
    }
    OUT_PREFLIGHT.parent.mkdir(parents=True, exist_ok=True)
    OUT_PREFLIGHT.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return report


def seven_word_shingles(value: str) -> set[tuple[str, ...]]:
    words = [word.casefold() for word in WORD_RE.findall(value)]
    return {
        tuple(words[index : index + 7])
        for index in range(max(0, len(words) - 6))
    }


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
    for index, article in enumerate(ARTICLES):
        body = render_article(article)
        public_text = f"{article['title']} {article['short']} {body}"
        visible = visible_text(body)
        if FORBIDDEN_PUBLIC_RE.search(public_text):
            raise SystemExit(f"Forbidden public wording in {article['title']}")
        if FIXED_PRICE_RE.search(visible_text(public_text)):
            raise SystemExit(f"Fixed price found in {article['title']}")
        one_character_paragraphs = [
            visible_text(value).strip()
            for value in re.findall(
                r"<p\b[^>]*>(.*?)</p>",
                body,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if len(visible_text(value).strip()) == 1
        ]
        metric = {
            "title": article["title"],
            "slug": article["link"],
            "words": len(WORD_RE.findall(visible)),
            "h2": len(re.findall(r"<h2\b", body, re.IGNORECASE)),
            "tables": len(re.findall(r"<table\b", body, re.IGNORECASE)),
            "responsive_tables": len(
                re.findall(
                    r'<div\b[^>]*style="[^"]*overflow-x:\s*auto',
                    body,
                    re.IGNORECASE,
                )
            ),
            "styled_blocks": len(
                re.findall(r"<div\b[^>]*style=", body, re.IGNORECASE)
            ),
            "action_buttons": len(
                re.findall(
                    r'<a\b[^>]*style="[^"]*display:\s*inline-block',
                    body,
                    re.IGNORECASE,
                )
            ),
            "faq_questions": len(article["faq"]),
            "one_character_paragraphs": len(one_character_paragraphs),
        }
        if metric["words"] < 2800:
            raise SystemExit(
                f"Article is too short: {article['title']} ({metric['words']} words)"
            )
        if (
            metric["h2"] < 24
            or metric["tables"] < 2
            or metric["responsive_tables"] != metric["tables"]
        ):
            raise SystemExit(f"Article structure is incomplete: {article['title']} ({metric})")
        if (
            metric["styled_blocks"] < 10
            or metric["action_buttons"] < 2
            or metric["faq_questions"] < 11
            or metric["one_character_paragraphs"]
        ):
            raise SystemExit(f"Article visual blocks are incomplete: {article['title']} ({metric})")
        metrics.append(metric)
        rendered.append(
            {
                "title": article["title"],
                "short": article["short"],
                "long": body,
                "link": article["link"],
                "date_posted": PUBLISH_DATE,
                "time_posted": f"{18 + index:02d}:00:00",
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
            overlaps.append(
                {
                    "left": left["title"],
                    "right": right["title"],
                    "score": round(score, 4),
                }
            )
            if score >= 0.13:
                raise SystemExit(
                    f"Article bodies overlap too much: {left['title']} / {right['title']} ({score:.4f})"
                )

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(rendered, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    report = preflight_links(rendered)
    if report["failure_count"]:
        failed = [check for check in report["checks"] if not check["ok"]]
        print(json.dumps({"failed_links": failed}, ensure_ascii=False, indent=2))
        raise SystemExit("Batch 51 link preflight failed")
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

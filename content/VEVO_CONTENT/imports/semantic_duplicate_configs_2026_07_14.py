"""Editorial configurations for VEVO semantic-overlap remediation."""

from __future__ import annotations

from typing import Any


IFRA = "https://ifrafragrance.org/initiatives-positions/safe-use-fragrance-science/ifra-standards/ifra-code-of-practice"
GINETEX = "https://www.ginetex.net/GB/labelling/care-symbols.asp"
LAUNDRY_HYGIENE = "https://pubmed.ncbi.nlm.nih.gov/33962979/"
LAUNDRY_MALODOUR = "https://pubmed.ncbi.nlm.nih.gov/39924526/"
EU_WASHING = "https://energy-efficient-products.ec.europa.eu/product-list/washing-machines_en"
EPA_CLEANING = "https://www.epa.gov/indoor-air-quality-iaq/biological-contaminants-and-indoor-air-quality"


FRAGRANCE_COMMERCE = {
    "category_title": "Vyberte si parfum do prania podľa charakteru vône",
    "category_body": "Začnite menšou dávkou podľa etikety a intenzitu posudzujte až na suchej bielizni.",
    "category_href": "/c/vevo-fragrance/parfum-do-prania",
    "product_title": "Parfum do prania Vevo No.07 Ylang Absolute",
    "product_body": "Konkrétna kvetinovo-orientálna voľba na porovnávací test pri bežnom praní.",
    "product_href": "/p-1531/parfum-do-prania-vevo-no-07-ylang-absolute",
}

LAUNDRY_COMMERCE = {
    "category_title": "Pracie gély pre čistý základ bielizne",
    "category_body": "Gél vyberajte podľa farby, materiálu, znečistenia a pokynov na ošetrovacom štítku.",
    "category_href": "/c/vevo-home-care/pranie/praci-gel",
    "product_title": "Prací gél z Marseillského mydla",
    "product_body": "Praktický základ bežného prania; dávku upravte podľa návodu, náplne a tvrdosti vody.",
    "product_href": "/p-1551/praci-gel-hypoalergenny-z-marseillskeho-mydla-1l",
}

CLEANING_COMMERCE = {
    "category_title": "Čistiace prostriedky pre pravidelnú údržbu",
    "category_body": "Vyberte prostriedok vhodný pre konkrétny povrch a pred použitím si prečítajte etiketu.",
    "category_href": "/c/vevo-home-care/upratovanie/cistiace-prostriedky",
    "product_title": "Biely ocot 1 liter",
    "product_body": "Možnosť na vybrané domáce čistenie po overení kompatibility povrchu; nikdy ho nemiešajte s chlórovým bielidlom.",
    "product_href": "/p-1560/biely-ocot-1-liter",
}


CONFIGS: list[dict[str, Any]] = [
    {
        "post_id": "2309",
        "title": "Parfum do prania pri ručnom praní",
        "slug": "parfum-do-prania-pri-rucnom-prani",
        "short": "Ako použiť parfum do prania pri ručnom praní bez priameho kontaktu koncentrátu s textilom, škvŕn a zbytočne silnej vône.",
        "description": "Ručné pranie a parfum do prania: správne riedenie, plákanie, dávkovanie, citlivé materiály a bezpečné sušenie.",
        "quick": "Pri ručnom praní nepridávajte koncentrát priamo na suchú látku. Najprv ho rozptýľte vo vode podľa etikety výrobku, textil ponorte rovnomerne, jemne premiešajte a dôkladne vypláchajte. Pri vlne, hodvábe alebo odeve so zákazom mokrého čistenia má vždy prednosť ošetrovací štítok.",
        "intro": "Tento návod rieši vaňu, umývadlo alebo nádobu, nie automatickú práčku. Pri ručnom praní chýba dávkovač aj riadené plákanie, preto je dôležitejšie poradie, rozptýlenie výrobku vo vode a šetrná manipulácia s mokrým vláknom.",
        "focus": "riedenie vo vode, poradie pracieho prostriedku a vône, ručné plákanie, citlivé kusy a sušenie bez krútenia",
        "boundary": "pranie na 30 °C v práčke, pranie na 60 °C a výber pracieho programu; tieto situácie majú vlastné návody",
        "points": [
            "koncentrát nesmie vytvoriť lokálnu mokrú škvrnu na suchom textile",
            "voda, výrobok aj textil sa spájajú v poradí uvedenom na etikete",
            "ručné plákanie musí odstrániť prací prostriedok bez hrubého žmýkania",
            "vôňu hodnotíme až po úplnom vysušení a vyvetraní",
        ],
        "sections": [
            (
                "Prečo sa ručné pranie správa inak než práčka",
                [
                    "Práčka dávku rozptýli vo veľkom objeme vody a bielizeň pohybuje opakovane. V nádobe vznikajú koncentrovanejšie zóny a človek ľahko naleje výrobok na jedno miesto. Preto je pri ručnom praní kľúčové dôkladné premiešanie ešte pred vložením textilu.",
                    "Mokré jemné vlákna sú citlivé na ťahanie, trenie a krútenie. Textil skôr stláčajte pod hladinou a po oplachu z neho vodu vytlačte cez uterák. Súvisiace symboly a maximálnu teplotu vysvetľuje <a href=\"/n/symboly-prania-kompletny-sprievodca-praciim-stitkom\">sprievodca pracími štítkami</a>.",
                ],
            ),
            (
                "Ako rozriediť vôňu bez fľakov",
                [
                    "Použite objem vody, v ktorom sa kus môže voľne pohybovať. Výrobok odmerajte podľa etikety a rozmiešajte v samostatnej čistej vode alebo spôsobom určeným výrobcom. Neimprovizujte vyššou dávkou len preto, že mokrý textil vonia slabo.",
                    "Ak sa objaví olejový kruh, nerovnomerná vôňa alebo klzký povrch, textil znovu jemne vypláchajte. Nepridávajte ďalší prací gél ani iný parfum, kým neviete, či ide o zvyšok výrobku alebo vlastnosť materiálu.",
                ],
            ),
            (
                "Jemné materiály a farebná stálosť",
                [
                    "Ručné pranie automaticky neznamená bezpečné pranie. Hodváb, vlna, viskóza, výšivky a nestále farby môžu reagovať na vodu, teplotu aj trenie. Najprv otestujte skrytý lem a pri uvoľňovaní farby kus nenechávajte namočený.",
                    "Ak štítok povoľuje iba profesionálne čistenie alebo zakazuje pranie, parfum do vody nepridávajte. Vôňa nie je dôvod obísť technologické obmedzenie textilu a prípadné poškodenie sa po vysušení nemusí dať vrátiť.",
                ],
            ),
            (
                "Plákanie, odvodnenie a sušenie",
                [
                    "Plákajte v čistej vode bez prudkého teplotného skoku. Textil nekrúťte; zložte ho, jemne vytlačte a pri citlivých úpletoch použite savý uterák. Zvyšková voda s výrobkom môže spôsobiť mapy, tuhosť alebo nerovnomernú vôňu.",
                    "Sušte spôsobom povoleným štítkom, ideálne s dostatkom vzduchu. Vôňu posúďte až po dosušení. Ak je príliš výrazná, ďalší pokus neriešte maskovaním, ale menšou dávkou a dôkladnejším rozptýlením.",
                ],
            ),
        ],
        "table": {
            "headers": ["Situácia", "Riziko", "Bezpečnejší postup"],
            "rows": [
                ["Koncentrát na látke", "lokálna mapa", "najprv rozptýliť vo vode"],
                ["Jemný úplet", "vyťahanie", "nestláčať silou ani nekrútiť"],
                ["Nestála farba", "púšťanie", "krátky test na skrytom leme"],
                ["Silná vôňa za mokra", "predávkovanie", "počkať na úplné vysušenie"],
            ],
        },
        "steps": [
            "Prečítajte štítok textilu aj etiketu výrobku.",
            "Pripravte čistú nádobu a vodu v povolenej teplote.",
            "Rozptýľte prací prostriedok a parfum podľa návodu, nie na látku.",
            "Textil jemne ponorte a stláčajte bez drhnutia.",
            "Dôkladne plákajte v čistej vode.",
            "Vodu vytlačte bez krútenia a vytvarujte kus.",
            "Výsledok posúďte až po úplnom vysušení.",
        ],
        "checks": [
            ["Materiál", "Povoľuje štítok ručné pranie a zvolenú teplotu?"],
            ["Rozptýlenie", "Je výrobok vo vode rovnomerne rozmiešaný bez kvapiek na látke?"],
            ["Oplach", "Nie je povrch po plákaní klzký, lepkavý alebo mapovitý?"],
        ],
        "expert": [
            "GINETEX uvádza, že symbol ručného prania zahŕňa rozpustenie jemného detergentu vo väčšom množstve vody, opatrný pohyb bez trenia a žmýkania, dôkladné plákanie a vytvarovanie. Toto je dôležitejšie než ľudové pravidlo, že každý citlivý kus stačí namočiť.",
            "Parfumová kompozícia je navrhnutá pre konkrétny spôsob použitia. Bezpečnostné limity surovín sa posudzujú podľa kategórie výrobku a expozície; preto nemožno parfum na pokožku, interiérový sprej a parfum do prania zamieňať.",
        ],
        "sources": [["GINETEX: symboly ošetrovania textilu", GINETEX], ["IFRA Code of Practice", IFRA]],
        "commerce": FRAGRANCE_COMMERCE,
        "faq": [
            ["Môžem naliať parfum priamo na mokré oblečenie?", "Nie. Koncentrát rozptýľte vo vode spôsobom uvedeným na etikete, aby nevznikla lokálna škvrna."],
            ["Koľko parfumu použiť pri jednom kuse?", "Riaďte sa etiketou a objemom vody; nezačínajte automaticky dávkou určenou pre plný bubon."],
            ["Patrí parfum do posledného plákania?", "Použite iba postup uvedený výrobcom konkrétneho produktu; jednotlivé formulácie sa môžu líšiť."],
            ["Môžem ručne prať hodváb?", "Iba ak to povoľuje štítok. Niektoré hodvábne kusy vyžadujú profesionálne čistenie."],
            ["Prečo je vôňa po vysušení nerovnomerná?", "Najčastejšie pre slabé rozptýlenie, príliš málo vody, nedostatočné plákanie alebo nerovnomerné sušenie."],
        ],
    },
    {
        "post_id": "2310",
        "title": "Parfum do prania pri praní na 30 stupňov",
        "slug": "parfum-do-prania-pri-prani-na-30-stupnov",
        "short": "Pranie na 30 °C chráni mnohé farby a syntetické materiály, no čistotu určuje aj program, dávka gélu, veľkosť náplne a sušenie.",
        "description": "Ako používať parfum do prania pri 30 °C: vhodné textílie, dávkovanie, nízkoteplotné pranie, pachy a správne sušenie.",
        "quick": "Pri 30 °C vyberte program podľa štítku, nepreplňte bubon, prací gél dávkujte podľa znečistenia a parfum do prania použite iba v určenej priehradke a množstve. Nízka teplota sama o sebe neznamená slabé pranie, ale potrebuje vhodný detergent, dostatok času a rýchle usušenie.",
        "intro": "Tento článok rieši bežné nízkoteplotné pranie tmavých, farebných, syntetických a mierne znečistených kusov. Nejde o ručné pranie ani o hygienicky náročnú náplň, pri ktorej môže byť potrebná vyššia teplota alebo iný režim.",
        "focus": "výber náplne pre 30 °C, rozpustenie a oplach gélu, pachy pri nízkej teplote, dávku parfumu a okamžité sušenie",
        "boundary": "ručné pranie a odolné uteráky či posteľná bielizeň prané na 60 °C; tieto režimy majú odlišné riziká",
        "points": [
            "tridsať stupňov je limit teploty, nie univerzálny program pre každý textil",
            "nízka teplota potrebuje správnu dávku detergentu a nepreplnený bubon",
            "parfum nevyrieši biologický alebo zatuchnutý pach z nedostatočného prania",
            "rýchle vybratie a sušenie je súčasťou výsledku",
        ],
        "sections": [
            (
                "Ktoré kusy dávajú pri 30 °C zmysel",
                [
                    "Na 30 °C sa často perú tmavé farby, syntetické zmesi, športové vrstvy a mierne znečistené bežné oblečenie, ak to povoľuje štítok. Jemný alebo krátky program však môže mať inú mechaniku, objem vody a plákanie než štandardný cyklus.",
                    "Nevkladajte automaticky do jednej náplne uteráky, spodnú bielizeň, mastné utierky a elastické športové kusy. Potrebujú odlišnú mechaniku a hygienickú úroveň. Rozdelenie podľa funkcie je dôležitejšie než snaha naplniť bubon za každú cenu.",
                ],
            ),
            (
                "Prečo môže bielizeň pri nízkej teplote zapáchať",
                [
                    "Pach môže zostať pri priveľkej náplni, krátkom cykle, poddávkovaní aj predávkovaní gélu alebo pri dlhom čakaní mokrej bielizne v bubne. Parfum do prania pach prekryje iba dočasne, ak zdroj ostane vo vláknach alebo práčke.",
                    "Pri opakovanom probléme skontrolujte tesnenie, zásobník a filter práčky a porovnajte dlhší program s rovnakou dávkou. Pomôže aj návod <a href=\"/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia\">prečo oblečenie zapácha po praní</a>.",
                ],
            ),
            (
                "Dávka parfumu pri 30 °C",
                [
                    "Začnite spodnou hranicou odporúčania na etikete, najmä pri syntetike a menšej náplni. Mokrá bielizeň môže pôsobiť menej voňavo než suchá, preto dávku nezvyšujte počas jedného cyklu podľa čuchu pri otvorení dvierok.",
                    "Ak je výsledok po vysušení príliš silný, ďalšie pranie nastavte s menšou dávkou. Ak je slabý, najprv overte čistotu, dávkovanie gélu, plákanie a sušenie. Intenzita nevzniká iba množstvom parfumu.",
                ],
            ),
            (
                "Sušenie po nízkoteplotnom praní",
                [
                    "Bielizeň vyberte hneď po skončení a rozložte tak, aby medzi kusmi prúdil vzduch. Pomalé sušenie v preplnenej miestnosti môže vytvoriť zatuchnutý tón, ktorý sa mieša s parfumom a pôsobí ťažšie.",
                    "Pri sušičke rešpektujte štítok a nezvoľte zbytočne vysokú teplotu. Vôňu posudzujte po vychladnutí; horúca textília uvoľňuje prchavé zložky inak než bielizeň uložená v skrini.",
                ],
            ),
        ],
        "table": {
            "headers": ["Náplň", "30 °C", "Čo skontrolovať"],
            "rows": [
                ["Tmavé oblečenie", "často vhodné", "štítok a farebnú stálosť"],
                ["Športová syntetika", "často vhodné", "dlhší oplach a rýchle sušenie"],
                ["Mastné utierky", "často slabé", "vyššiu teplotu podľa štítku"],
                ["Jemná bielizeň", "podľa kusu", "jemný program a nízke otáčky"],
            ],
        },
        "steps": [
            "Roztrieďte náplň podľa materiálu, farby a znečistenia.",
            "Overte na štítku, že 30 °C a zvolený program sú povolené.",
            "Naplňte bubon tak, aby sa textil mohol pohybovať.",
            "Odmerajte prací gél podľa návodu a tvrdosti vody.",
            "Parfum vložte iba do určenej priehradky a v odporúčanej dávke.",
            "Po skončení náplň okamžite vyberte a rovnomerne usušte.",
            "Intenzitu a čistotu posúďte až na suchej bielizni.",
        ],
        "checks": [
            ["Čistota", "Zmizlo znečistenie a pach ešte pred hodnotením parfumu?"],
            ["Oplach", "Nie je textil klzký, tuhý alebo pokrytý bielymi mapami?"],
            ["Sušenie", "Bola náplň vybratá bez čakania a vysušená s prúdením vzduchu?"],
        ],
        "expert": [
            "Účinnosť prania vzniká kombináciou času, teploty, chémie a mechaniky. Zníženie teploty možno pri bežnom znečistení kompenzovať vhodným detergentom a programom, nemožno však predpokladať rovnaký hygienický výsledok pri každej náplni.",
            "Výskum domáceho prania upozorňuje, že správanie mikroorganizmov ovplyvňuje celý proces vrátane detergentu, plákania a sušenia. Vôňa je senzorická vrstva nad čistotou, nie náhrada prania.",
        ],
        "sources": [["GINETEX: symboly a maximálne teploty", GINETEX], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
        "commerce": FRAGRANCE_COMMERCE,
        "faq": [
            ["Je 30 °C dosť na každé oblečenie?", "Nie. Rozhoduje štítok, znečistenie, materiál a požadovaná hygiena."],
            ["Prečo parfum pri 30 °C nevonia?", "Najprv skontrolujte čistotu, dávku, preplnenie, oplach a spôsob sušenia."],
            ["Môžem zvýšiť dávku pri krátkom programe?", "Nie automaticky. Kratší cyklus môže horšie oplachovať; riaďte sa návodom výrobku a práčky."],
            ["Je 30 °C vhodných na športové oblečenie?", "Často áno, ak to povoľuje štítok; dôležitý je vhodný gél a rýchle sušenie."],
            ["Kedy zvoliť vyššiu teplotu?", "Pri odolnej hygienicky náročnej náplni, ak ju povoľuje textilný štítok."],
        ],
    },
    {
        "post_id": "2311",
        "title": "Parfum do prania pri praní na 60 stupňov",
        "slug": "parfum-do-prania-pri-prani-na-60-stupnov",
        "short": "Pranie na 60 °C patrí len odolným textíliám, ktorých štítok túto teplotu povoľuje; vôňa sa rieši až po správnom hygienickom cykle.",
        "description": "Parfum do prania pri 60 °C: vhodné uteráky a posteľná bielizeň, hygiena, dávkovanie, teplo, sušenie a ochrana textilu.",
        "quick": "Šesťdesiat stupňov použite iba na odolnú náplň s povolením na štítku, napríklad vybrané uteráky, posteľnú bielizeň alebo utierky. Parfum do prania dávkujte podľa etikety, nezvyšujte ho pre vyššiu teplotu a výsledok hodnotte až po vysušení a vychladnutí.",
        "intro": "Tento návod sa sústreďuje na odolnú hygienicky náročnejšiu náplň. Neplatí pre elastické športové materiály, jemnú bielizeň, tmavé nestále farby ani kúsky s membránou, potlačou či lepenými detailmi.",
        "focus": "výber textílií pre 60 °C, hygienický cieľ, ochranu vlákien, primeranú dávku vône a sušenie bez presušenia",
        "boundary": "nízkoteplotné pranie na 30 °C a ručné pranie citlivých kusov, kde sa používajú iné programy a mechanika",
        "points": [
            "šesťdesiat stupňov sa volí podľa štítku a hygienickej potreby, nie podľa zvyku",
            "vyššia teplota nenahrádza detergent, čas ani správne naplnenie bubna",
            "parfum sa nezvyšuje automaticky preto, že program používa teplejšiu vodu",
            "presušenie môže poškodiť vlákno aj zmeniť vnímanie vône",
        ],
        "sections": [
            (
                "Čo možno prať na 60 °C",
                [
                    "Typickými kandidátmi sú odolné bavlnené uteráky, posteľná bielizeň a kuchynské textílie, ale iba keď štítok povoľuje 60 °C. Farebný pigment, elastan, potlač alebo povrchová úprava môžu limit znížiť.",
                    "Jedna náplň by mala mať podobný materiál, farbu a hygienický cieľ. Nepridávajte športové legíny alebo podprsenku len preto, že v bubne zostalo miesto. Vysoká teplota môže urýchliť stratu pružnosti a farby.",
                ],
            ),
            (
                "Hygiena nie je iba číslo na displeji",
                [
                    "Reálny výsledok ovplyvňuje dosiahnutá teplota, čas, detergent, mechanika, množstvo vody a sušenie. Eco program môže pracovať inak než rýchly cyklus a používateľ nemá hodnotiť hygienu iba podľa názvu programu.",
                    "Pri bežnej domácnosti nie je potrebné prať všetko na 60 °C. Vyššiu teplotu rezervujte pre odolné kusy a situácie, kde má hygienický prínos. Pri chorobe alebo osobitnom riziku sa riaďte odbornými odporúčaniami pre konkrétnu situáciu.",
                ],
            ),
            (
                "Ako sa pri teple mení vôňa",
                [
                    "Prchavé zložky sa pri zahriatí uvoľňujú rýchlejšie, no konečný profil závisí od formulácie, oplachu a vlákna. Silná vôňa pri otvorení horúceho bubna preto nepredpovedá, čo zostane na suchej bielizni.",
                    "Nepridávajte extra parfum ako kompenzáciu teploty. Použite odporúčané množstvo a porovnajte dve rovnaké náplne. Ak vôňa slabne, overte aj presušenie, tvrdosť vody, dávku gélu a zvyšky vo vláknach.",
                ],
            ),
            (
                "Uteráky a posteľná bielizeň po cykle",
                [
                    "Odolný uterák potrebuje dôkladné plákanie a úplné usušenie. Priveľa výrobku môže znížiť savosť alebo vytvoriť tuhý povrch. Praktické riešenia rozoberá článok <a href=\"/n/ako-zmaekcit-uteraky\">ako zmäkčiť uteráky</a>.",
                    "Posteľnú bielizeň vyberte bez dlhého čakania, rozložte a dosušte. Pri sušičke rešpektujte povolenú teplotu. Horúce kusy nechajte vychladnúť pred skladaním, aby ste neuzavreli zvyškovú vlhkosť v skrini.",
                ],
            ),
        ],
        "table": {
            "headers": ["Textil", "60 °C", "Hlavné riziko"],
            "rows": [
                ["Bavlnený uterák", "často povolené", "presušenie a nánosy"],
                ["Posteľná bielizeň", "podľa štítku", "zrazenie a strata farby"],
                ["Športová syntetika", "často nevhodné", "poškodenie pružnosti"],
                ["Jemná bielizeň", "zvyčajne nevhodné", "deformácia a poškodenie"],
            ],
        },
        "steps": [
            "Vyberte iba odolné kusy s povolenou teplotou 60 °C.",
            "Roztrieďte farby a oddeľte elastické či jemné materiály.",
            "Zvoľte program podľa hygienického cieľa a návodu práčky.",
            "Odmerajte detergent podľa náplne a tvrdosti vody.",
            "Parfum dávkujte podľa etikety bez automatického navyšovania.",
            "Po cykle náplň vyberte a úplne usušte.",
            "Vôňu, savosť a stav vlákien posúďte po vychladnutí.",
        ],
        "checks": [
            ["Povolenie", "Má každý kus na štítku maximálnu teplotu aspoň 60 °C?"],
            ["Hygienický cieľ", "Je vyššia teplota pre túto náplň skutočne potrebná?"],
            ["Funkcia textilu", "Zostala po cykle savosť, pružnosť a farba bez zhoršenia?"],
        ],
        "expert": [
            "Systematické práce o domácom praní opisujú teplotu ako jednu z viacerých premenných. Hygienický výkon sa mení podľa detergentu, času, mechaniky, pôvodného znečistenia a následného sušenia; samotné číslo programu nie je úplná odpoveď.",
            "Maximálna teplota na symbole prania sa nesmie prekročiť. GINETEX zároveň rozlišuje normálny, mierny a veľmi mierny proces, takže dve náplne pri rovnakej teplote nemusia znášať rovnakú mechaniku.",
        ],
        "sources": [["GINETEX: symboly a maximálne teploty", GINETEX], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
        "commerce": FRAGRANCE_COMMERCE,
        "faq": [
            ["Musím pri 60 °C použiť viac parfumu?", "Nie. Dávku neurčuje samotná teplota; riaďte sa etiketou, veľkosťou náplne a výsledkom na suchej bielizni."],
            ["Môžem prať športové oblečenie na 60 °C?", "Iba ak to výslovne povoľuje štítok; elastické a funkčné materiály často potrebujú nižšiu teplotu."],
            ["Je 60 °C vždy hygienickejších?", "Hygienu ovplyvňuje celý cyklus, detergent, čas aj sušenie, nie iba nastavená teplota."],
            ["Prečo uteráky po praní menej sajú?", "Častou príčinou sú nánosy výrobkov, predávkovanie, slabé plákanie alebo presušenie."],
            ["Kedy hodnotiť vôňu?", "Až po úplnom vysušení a vychladnutí textilu."],
        ],
    },
]

CONFIGS.extend(
    [
        {
            "post_id": "2313",
            "title": "Prací gél na čierne oblečenie",
            "slug": "praci-gel-na-cierne-oblecenie",
            "short": "Gél na čierne oblečenie má dobre oplachovať pri nižšej teplote a nezanechať biele mapy; vyblednuté farbivo však nedokáže obnoviť.",
            "description": "Ako vybrať a dávkovať prací gél na čierne oblečenie, predísť bielym mapám, strate farby, nánosom a poškodeniu tmavých textílií.",
            "quick": "Na čierne oblečenie zvoľte tekutý prací prostriedok vhodný pre tmavé farby, perte naruby pri teplote povolenej štítkom, nepreplňte bubon a nepredávkujte. Biela mapa nemusí byť vyblednutie; často ide o nerozpustený prostriedok, prehyb, oder alebo minerálny zvyšok.",
            "intro": "Tento článok rieši zachovanie tmavého vzhľadu a diagnostiku bielych stôp. Samostatný návod na biele oblečenie sa venuje zašednutiu a škvrnám, kým farebná bielizeň potrebuje prácu s púšťaním rôznych pigmentov.",
            "focus": "ochranu tmavého farbiva, rozdiel medzi vyblednutím a povlakom, dávku gélu, trenie, plákanie a sušenie mimo ostrého slnka",
            "boundary": "rozjasňovanie bielej bielizne a bezpečné miešanie viacerých sýtych farieb; tieto ciele vyžadujú iné rozhodovanie",
            "points": [
                "stratené farbivo prací gél nevráti, ale správny proces môže ďalšiu stratu spomaliť",
                "biele mapy najprv diagnostikujte, až potom meňte výrobok",
                "tmavé kusy perte naruby a s priestorom na pohyb aj plákanie",
                "vysoká teplota, trenie a priame slnko môžu urýchliť zmenu vzhľadu",
            ],
            "sections": [
                (
                    "Čo má gél na čierne oblečenie zvládnuť",
                    [
                        "Praktický gél na tmavú bielizeň sa musí rozptýliť pri zvolenej teplote, odstrániť bežné nečistoty a dobre sa vypláchnuť. Marketingové označenie samo osebe nezaručuje vhodnosť pre každý materiál; rozhoduje etiketa výrobku a štítok odevu.",
                        "Tekutá forma môže znížiť riziko viditeľných nerozpustených častíc, no aj gél vytvorí mapy pri predávkovaní, preplnení alebo slabom plákaní. Dávku neupravujte podľa farby látky, ale podľa návodu, znečistenia, náplne a tvrdosti vody.",
                    ],
                ),
                (
                    "Vyblednutie, oder alebo prací povlak",
                    [
                        "Vyblednutie je zmena samotného pigmentu a býva trvalejšia. Oder sa objavuje na švoch, kolenách, golieri alebo miestach trenia. Prací povlak sa často sústreďuje v záhyboch a môže sa po opätovnom oplachu zmenšiť.",
                        "Najprv urobte skúšku vlhkou tmavou handričkou a samostatný oplach bez ďalšieho gélu. Ak stopa mizne, pravdepodobnejší je zvyšok. Ak zostáva na exponovaných miestach a štruktúra je hladká, môže ísť o stratu farbiva alebo mechanické opotrebovanie.",
                    ],
                ),
                (
                    "Triedenie a mechanika cyklu",
                    [
                        "Čierne džínsy, jemná viskóza a športová syntetika nemusia patriť do jedného programu. Rozdeľujte podľa materiálu, hmotnosti a povrchovej úpravy. Ťažké zipsy a hrubé švy zvyšujú trenie o jemné kusy.",
                        "Odevy obráťte naruby, zapnite zipsy a citlivé kúsky vložte do vrecka na pranie. Bubnu nechajte priestor. Príliš malá náplň je neefektívna, no natlačená bielizeň sa horšie perie aj oplachuje.",
                    ],
                ),
                (
                    "Sušenie a skladovanie tmavej bielizne",
                    [
                        "Dlhé sušenie na ostrom slnku môže meniť tmavé pigmenty, najmä pri nestálych farbách. Sušte podľa štítku, ideálne naruby a bez zbytočne vysokej teploty. Pri sušičke sa vyhnite presušeniu.",
                        "Pred uložením nechajte odev úplne vyschnúť. Vlhký tmavý textil v koši alebo skrini môže zatuchnúť; ďalšia dávka vône príčinu neodstráni. Pri opakovaných mapách skontrolujte aj zásobník a bubon práčky.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Prejav", "Pravdepodobná príčina", "Prvý test"],
                "rows": [
                    ["Biela mapa v záhybe", "zvyšok gélu", "samostatný oplach"],
                    ["Svetlé švy", "oder", "porovnať exponované miesta"],
                    ["Celkové zosivenie", "strata pigmentu", "porovnať vnútorný lem"],
                    ["Klzký povrch", "predávkovanie", "znížiť dávku a náplň"],
                ],
            },
            "steps": [
                "Skontrolujte štítok, vrecká, zipsy a nové nestále kusy.",
                "Rozdeľte tmavé oblečenie podľa materiálu a hmotnosti.",
                "Otočte kusy naruby a zvoľte povolenú teplotu.",
                "Odmerajte gél podľa návodu a tvrdosti vody.",
                "Nechajte v bubne priestor na pohyb a oplach.",
                "Po cykle oblečenie vyberte bez čakania a sušte naruby.",
                "Mapy diagnostikujte až na suchom textile pri dennom svetle.",
            ],
            "checks": [
                ["Pigment", "Je stopa aj na vnútornom leme, alebo iba na miestach trenia?"],
                ["Zvyšok", "Zmenší sa mapa po čistom oplachu bez ďalšieho výrobku?"],
                ["Proces", "Mal bubon dostatok miesta a primeranú dávku gélu?"],
            ],
            "expert": [
                "Farebná stálosť závisí od farbiva, väzby na vlákno, materiálu a následnej úpravy. Domáce pranie ju ovplyvňuje teplotou, alkalitou, mechanickým oderom a počtom cyklov; jeden univerzálny trik preto nefunguje na všetky čierne textílie.",
                "GINETEX uvádza pre tmavé farebné textílie konkrétne mierne cykly a maximálne teploty. Číslo vo vaničke je horný limit, nie odporúčanie automaticky použiť najvyššiu povolenú teplotu.",
            ],
            "sources": [["GINETEX: symboly prania a farebné cykly", GINETEX], ["Európska komisia: práčky a údaje na energetickom štítku", EU_WASHING]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Vráti gél čiernemu tričku pôvodnú farbu?", "Nie. Môže pomôcť obmedziť ďalšie zaťaženie, ale stratené farbivo neobnoví."],
                ["Prečo sú na čiernom oblečení biele škvrny?", "Často ide o zvyšok výrobku, preplnenie, slabý oplach alebo minerálny povlak."],
                ["Mám prať čierne oblečenie naruby?", "Áno, často to znižuje priame trenie lícnej strany, ak štítok neurčuje inak."],
                ["Je tekutý gél vždy bez máp?", "Nie. Pri predávkovaní alebo slabom plákaní môže zanechať viditeľný povlak."],
                ["Môžem sušiť čierne oblečenie na slnku?", "Podľa štítku, no dlhé ostré slnko môže pri nestálych farbách urýchliť zmenu vzhľadu."],
            ],
        },
        {
            "post_id": "2314",
            "title": "Prací gél na biele oblečenie",
            "slug": "praci-gel-na-biele-oblecenie",
            "short": "Gél na bielu bielizeň má odstrániť nečistoty bez nánosov; zašednutie, žlté mapy a lokálne škvrny však potrebujú rozdielnu diagnostiku.",
            "description": "Ako vybrať prací gél na biele oblečenie, riešiť zašednutie, žlté škvrny, tvrdú vodu, predpieranie a bezpečné bielenie podľa štítku.",
            "quick": "Biele oblečenie perte oddelene od farebného, zvoľte gél podľa materiálu a typu škvŕn, nepreplňte bubon a rešpektujte symbol bielenia. Celkové zašednutie sa rieši inak než žlté podpazušie alebo jedna mastná škvrna; vyššia dávka gélu bez diagnózy môže problém zhoršiť.",
            "intro": "Biela nie je jeden materiál. Bavlnená plachta, polyesterová košeľa a elastická spodná bielizeň môžu vyzerať rovnako svetlo, no potrebujú inú teplotu, mechaniku aj prístup k bieleniu.",
            "focus": "triedenie bielej náplne, rozdiel medzi zašednutím a škvrnou, vhodný gél, tvrdú vodu, predpieranie a symboly bielenia",
            "boundary": "ochrana čierneho pigmentu a pranie sýtych farebných kusov, kde je cieľom obmedziť prenos farbiva",
            "points": [
                "biele oblečenie sa triedi aj podľa materiálu a znečistenia, nie iba farby",
                "celkové zašednutie, žltá mapa a lokálna škvrna majú odlišné príčiny",
                "bielidlo použite iba vtedy, keď ho povoľuje štítok aj etiketa výrobku",
                "predávkovanie gélu môže zanechať povlak a zachytiť ďalšie nečistoty",
            ],
            "sections": [
                (
                    "Prečo biela bielizeň šedne alebo žltne",
                    [
                        "Zašednutie môže súvisieť s prenosom nečistôt v zmiešanej náplni, tvrdou vodou, nánosom detergentu alebo príliš nízkou dávkou pre veľmi špinavé pranie. Žlté mapy zasa často vznikajú kombináciou potu, kozmetiky, mazu a tepla.",
                        "Najprv porovnajte rovnomernosť zmeny. Celý kus potrebuje úpravu procesu, lokálna škvrna predčistenie. Nelejte koncentrovaný gél ani bielidlo priamo na textil bez postupu výrobcu.",
                    ],
                ),
                (
                    "Univerzálny gél, gél na biele a bieliaca zložka",
                    [
                        "Označenie na obale čítajte spolu so zložením a dávkovaním. Nie každý tekutý gél obsahuje rovnaký systém na udržanie belosti a nie každý biely materiál toleruje bieliacu zložku. Elastan a potlače môžu byť citlivé.",
                        "Ak štítok povoľuje iba kyslíkové bielenie, chlórový výrobok nepoužívajte. Prázdny trojuholník, trojuholník s čiarami a prečiarknutý trojuholník majú odlišný význam; pri neistote si pozrite <a href=\"/n/symboly-prania-kompletny-sprievodca-praciim-stitkom\">symboly prania</a>.",
                    ],
                ),
                (
                    "Predčistenie bez zapečenia škvrny",
                    [
                        "Škvrnu identifikujte ešte pred praním. Bielkovinové stopy, mastnota, kozmetika a pigmentované jedlo reagujú odlišne. Horúca voda môže niektoré škvrny zafixovať, preto nezačínajte najvyššou teplotou automaticky.",
                        "Predčistič otestujte na skrytom mieste a dodržte čas pôsobenia. Textil nenechajte s výrobkom vyschnúť, ak to etiketa nepovoľuje. Po cykle škvrnu skontrolujte pred sušičkou alebo žehlením.",
                    ],
                ),
                (
                    "Tvrdá voda, dávka a plákanie",
                    [
                        "Minerály vo vode menia výkon detergentu a môžu prispieť k povlaku. Dávku nastavte podľa miestnej tvrdosti a náplne, nie odhadom. Priveľa gélu sa nemusí vypláchnuť a biely textil môže pôsobiť matne.",
                        "Bubon naplňte primerane a zvoľte dostatočný program. Ak je bielizeň po praní klzká alebo tuhá, urobte kontrolný oplach. Až potom rozhodujte, či treba zmeniť výrobok alebo dávkovanie.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Problém", "Častá príčina", "Prvý krok"],
                "rows": [
                    ["Celkové zašednutie", "zmiešaná náplň alebo povlak", "oddeliť biele a skontrolovať dávku"],
                    ["Žlté podpazušie", "pot a kozmetika", "lokálne predčistenie"],
                    ["Jedna mastná škvrna", "olejový zvyšok", "ošetriť pred teplom"],
                    ["Matný tvrdý povrch", "minerály alebo nános", "kontrolný oplach"],
                ],
            },
            "steps": [
                "Rozdeľte biele kusy podľa materiálu, znečistenia a povolenej teploty.",
                "Identifikujte lokálne škvrny a ošetrite ich pred hlavným cyklom.",
                "Skontrolujte symbol bielenia a obmedzenia potlače či elastanu.",
                "Odmerajte gél podľa náplne a tvrdosti vody.",
                "Zvoľte program s dostatkom času, pohybu a oplachu.",
                "Pred sušením skontrolujte, či škvrny naozaj zmizli.",
                "Belosť hodnotte na suchom textile pri prirodzenom svetle.",
            ],
            "checks": [
                ["Typ zmeny", "Je problém rovnomerný, alebo ide o jednu konkrétnu škvrnu?"],
                ["Povolenie", "Dovoľuje štítok zvolenú teplotu a typ bielenia?"],
                ["Zvyšky", "Nie je bielizeň po cykle klzká, tuhá alebo matná?"],
            ],
            "expert": [
                "Optický vzhľad bielej ovplyvňuje odraz svetla, zvyškové nečistoty, minerálny povlak aj prípadné optické zjasňovače. Rovnaký vizuálny problém preto môže mať chemicky odlišnú príčinu a vyžadovať iný zásah.",
                "Symbol bielenia podľa GINETEX určuje, či je povolené akékoľvek bielidlo, iba kyslíkové bielidlo alebo žiadne bielenie. Maximálna teplota prania je samostatné obmedzenie a nemá sa zamieňať s povolením bieliť.",
            ],
            "sources": [["GINETEX: pranie a symboly bielenia", GINETEX], ["Európska komisia: údaje o pracích cykloch", EU_WASHING]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Je gél na biele vhodný na každý biely kus?", "Nie. Rozhoduje materiál, potlač, elastan, štítok a povolené bielenie."],
                ["Prečo biele tričko žltne v podpazuší?", "Často ide o kombináciu potu, kožného mazu, antiperspirantu a tepla."],
                ["Mám pridať viac gélu na zašednutie?", "Nie bez diagnózy. Predávkovanie môže vytvoriť ďalší povlak a zhoršiť oplach."],
                ["Môžem použiť chlórové bielidlo?", "Iba ak ho povoľuje štítok textilu aj etiketa výrobku; nikdy ho nemiešajte s inými čističmi."],
                ["Kedy skontrolovať škvrnu?", "Pred sušičkou alebo žehlením, pretože teplo môže zvyšok zafixovať."],
            ],
        },
        {
            "post_id": "2315",
            "title": "Prací gél na farebné oblečenie",
            "slug": "praci-gel-na-farebne-oblecenie",
            "short": "Farebná bielizeň potrebuje gél, ktorý odstráni nečistoty bez zbytočného zaťaženia pigmentu; najdôležitejšie je však správne triedenie.",
            "description": "Ako vybrať prací gél na farebné oblečenie, triediť nové a sýte farby, riešiť púšťanie pigmentu, škvrny a farebnú stálosť.",
            "quick": "Farebné oblečenie rozdeľte na svetlé, tmavé a sýte nestále kusy, nové oblečenie perte prvé cykly opatrne a gél dávkujte podľa návodu. Obrúsok na zachytávanie farby môže byť doplnok, nie povolenie zmiešať červenú, bielu a tmavomodrú náplň.",
            "intro": "Cieľom nie je iba zabrániť jednému dramatickému zafarbeniu. Správny proces obmedzuje aj postupné blednutie, sivý závoj, škvrny po nedostatočnom vypraní a mechanické poškodenie povrchu farebného textilu.",
            "focus": "triedenie farieb, prvé prania nových kusov, farebnú stálosť, vhodnú teplotu, odstraňovanie škvŕn a limity zachytávačov farby",
            "boundary": "biela bielizeň so zašednutím a čierna bielizeň s viditeľnými mapami; tieto problémy majú osobitnú diagnostiku",
            "points": [
                "farebná bielizeň nie je jedna skupina: svetlé, tmavé a sýte nestále farby oddeľujte",
                "nový kus môže púšťať farbu aj pri správnom géle a nízkej teplote",
                "zachytávač farby znižuje riziko, ale nenahrádza rozumné triedenie",
                "škvrnu ošetrite cielene bez plošného zvyšovania dávky gélu",
            ],
            "sections": [
                (
                    "Ako triediť farebnú náplň",
                    [
                        "Rozdeľujte aspoň svetlé pastelové, stredné a tmavé sýte farby. Červené, fialové, tmavomodré a nové džínsové kusy môžu vyžadovať samostatný prvý cyklus. Zohľadnite aj materiál a hmotnosť, nie iba odtieň.",
                        "Biely detail na farebnom odeve zvyšuje nároky na stálosť. Ak si nie ste istí, otestujte vnútorný lem navlhčenou bielou handričkou. Uvoľnenie pigmentu je signál pre samostatné a krátke pranie podľa štítku.",
                    ],
                ),
                (
                    "Čo znamená gél na farebné oblečenie",
                    [
                        "Takýto gél je určený na bežné farebné textílie a zvyčajne sa volí bez potreby agresívneho rozjasňovania bielej. Konkrétne zloženie a vhodnosť pre vlnu, hodváb či funkčné materiály však určuje etiketa, nie samotný názov kategórie.",
                        "Dávku nastavte podľa znečistenia, náplne a tvrdosti vody. Plošné zvýšenie pri jednej škvrne zhorší oplach celej náplne. Lokálnu mastnotu alebo pigment riešte predčistením kompatibilným s farbou.",
                    ],
                ),
                (
                    "Nové oblečenie a púšťanie farby",
                    [
                        "Prebytočné alebo slabo viazané farbivo sa môže uvoľňovať najmä v prvých cykloch. Nižšia teplota a kratší kontakt môžu riziko znížiť, no nie sú zárukou. Kus nenechávajte po praní ležať mokrý v kontakte so svetlým textilom.",
                        "Ak farba pustila, nedávajte zafarbenú bielizeň do sušičky. Oddelte zdroj, postupujte podľa materiálu a riešte prenos skôr, než ho teplo zafixuje. Neexperimentujte miešaním viacerých odfarbovačov.",
                    ],
                ),
                (
                    "Zachytávač farby a jeho hranice",
                    [
                        "Zachytávací obrúsok môže viazať časť farbiva vo vode, jeho kapacita však nie je neobmedzená. Neviete podľa neho vopred zaručiť bezpečnosť konkrétnej sýtej farby ani ochrániť každý biely detail.",
                        "Používajte ho ako doplnok k triedeniu a pokynom výrobcu. Ak po cykle vidíte výrazne zafarbený obrúsok, ďalšie prania rizikového kusa robte samostatne, kým sa uvoľňovanie nezníži.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Skupina", "Príklad", "Odporúčanie"],
                "rows": [
                    ["Svetlé farby", "pastelová a béžová", "oddelene od tmavých"],
                    ["Sýte farby", "červená a fialová", "prvé cykly opatrne"],
                    ["Tmavé farby", "modrá a zelená", "podobné odtiene spolu"],
                    ["Nový nestály kus", "džínsy", "samostatný testovací cyklus"],
                ],
            },
            "steps": [
                "Rozdeľte svetlé, tmavé, sýte a nové rizikové kusy.",
                "Skontrolujte materiál, štítok, vrecká a farebnú stálosť.",
                "Ošetrite lokálne škvrny kompatibilným postupom.",
                "Odmerajte gél podľa návodu, náplne a tvrdosti vody.",
                "Zvoľte povolenú teplotu a nechajte bubnu priestor.",
                "Po skončení kusy okamžite oddeľte a usušte.",
                "Farbu posúďte za denného svetla až po vysušení.",
            ],
            "checks": [
                ["Stálosť", "Uvoľňuje nový kus pigment pri teste na skrytom leme?"],
                ["Triedenie", "Sú svetlé časti chránené pred sýtymi nestálymi kusmi?"],
                ["Dávka", "Zodpovedá množstvo gélu náplni a tvrdosti vody?"],
            ],
            "expert": [
                "Prenos farbiva závisí od typu farbiva, väzby na vlákno, teploty, času, mechaniky a zloženia pracieho kúpeľa. Dve tričká rovnakej farby preto nemusia mať rovnakú stálosť ani po rovnakom počte cyklov.",
                "GINETEX rozlišuje normálne a mierne farebné pracie procesy a uvádza maximálne teploty. Štítok je hornou hranicou bezpečnej starostlivosti; pre nový nestály kus možno zvoliť šetrnejší postup.",
            ],
            "sources": [["GINETEX: farebné pracie procesy", GINETEX], ["Európska komisia: práčky a pracie cykly", EU_WASHING]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Môžem prať všetky farby spolu na 30 °C?", "Nie. Nízka teplota nezaručí, že nový alebo nestály kus nepustí pigment."],
                ["Stačí použiť obrúsok na zachytávanie farby?", "Nie. Je to doplnok, ktorý nenahrádza triedenie a nemá neobmedzenú kapacitu."],
                ["Je gél na farebné vhodný na vlnu?", "Iba ak to výslovne povoľuje etiketa výrobku a štítok textilu."],
                ["Čo urobiť, keď farba pustila?", "Nesušte teplom, oddelte zdroj a riešte prenos podľa materiálu čo najskôr."],
                ["Prečo farby blednú aj pri dobrom géle?", "Farbu ovplyvňuje aj trenie, teplota, svetlo, počet cyklov a pôvodná farebná stálosť."],
            ],
        },
    ]
)

CONFIGS.extend(
    [
        {
            "post_id": "1500",
            "title": "Ako umyť okná bez šmúh – Kompletný sprievodca",
            "slug": "ako-umyt-okna-bez-smuh-kompletny-sprievodca",
            "short": "Kompletný pracovný postup umývania okien od rámu po finálne leštenie: poradie, náradie, malé zóny, čistá voda a kontrola v bočnom svetle.",
            "description": "Ako umyť okná bez šmúh krok za krokom: rámy, sklo, náradie, čistiaci roztok, stierka, mikrovlákno, počasie a bezpečnosť.",
            "quick": "Najprv odstráňte prach z rámu a parapetu, potom umývajte sklo v malých zónach čistým náradím a nepreháňajte množstvo prostriedku. Stierku po každom ťahu utrite, okraje dosušte samostatnou handričkou a výsledok skontrolujte z viacerých uhlov po úplnom odparení vody.",
            "intro": "Tento článok je lineárny pracovný postup od prípravy po kontrolu. Samostatný článok s častými otázkami je diagnostická príručka pre situáciu, keď už na skle zostali kruhy, mastný film alebo mapy.",
            "focus": "poradie práce, prípravu rámov, čisté náradie, malé zóny, techniku stierky, okraje, počasie a bezpečný prístup",
            "boundary": "diagnostiku konkrétnych druhov šmúh po dokončení a rozhodovanie, ktorá chyba ich pravdepodobne vytvorila",
            "points": [
                "rám a parapet sa čistia pred sklom, aby naň nepadal nový prach",
                "viac čističa neznamená menej šmúh a môže vytvoriť film",
                "stierka, handrička a voda musia zostať čisté počas celej práce",
                "bezpečnosť pri výške má prednosť pred dokonalým okrajom zvonka",
            ],
            "sections": [
                (
                    "Príprava priestoru a rámu",
                    [
                        "Odložte predmety z parapetu, zakryte citlivú podlahu a pripravte dve odlišné handričky: jednu na rám a druhú iba na sklo. Suchý prach povysávajte alebo zotrite skôr, než pridáte vodu.",
                        "Rám čistite prostriedkom kompatibilným s jeho materiálom. Drevo, lakovaný hliník a plast nemusia tolerovať rovnakú chémiu. Prípravok otestujte na skrytom mieste a nenechávajte ho zaschnúť na tesnení.",
                    ],
                ),
                (
                    "Umývanie skla v malých zónach",
                    [
                        "Sklo rovnomerne navlhčite bez stekania do rámu. Pri mastnom filme najprv uvoľnite nečistotu jemným pohybom a znečistenú vodu vymeňte. Jednou špinavou handričkou iba presúvate mastnotu po celej ploche.",
                        "Pracujte zhora nadol a veľké okno rozdeľte na dosiahnuteľné časti. Nedovoľte, aby roztok na jednej polovici zaschol, kým čistíte druhú. V horúcom priamom slnku sa odparovanie zrýchľuje a zvyšuje riziko máp.",
                    ],
                ),
                (
                    "Technika stierky a okrajov",
                    [
                        "Gumu stierky skontrolujte, či nie je poškodená alebo znečistená. Po každom ťahu ju utrite čistou handričkou. Jednotlivé dráhy mierne prekrývajte a udržujte stabilný tlak bez prudkého odskakovania.",
                        "Okraje a rohy dosušte samostatnou suchou handričkou, ktorá nebola použitá na rám. Neleštite celý povrch opakovane mokrým mikrovláknom; pri zvyšku filmu by ste vytvorili kruhy.",
                    ],
                ),
                (
                    "Kontrola a bezpečnosť",
                    [
                        "Počkajte, kým sklo úplne vyschne, a pozrite sa v bočnom svetle z interiéru aj zvonka. Označte si stranu jedným zvislým alebo vodorovným smerom leštenia, aby ste vedeli, kde sa stopa nachádza.",
                        "Na výšku používajte stabilné vybavenie určené na danú prácu a nevychádzajte na improvizované stoličky či parapety. Pri nedostupnom vonkajšom skle zvážte profesionálne čistenie.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Fáza", "Náradie", "Kontrolný bod"],
                "rows": [
                    ["Rám", "vysávač a rámová handrička", "bez prachu na skle"],
                    ["Mytie", "čistá podložka", "roztok nesmie zaschnúť"],
                    ["Stieranie", "nepoškodená stierka", "utrieť po každom ťahu"],
                    ["Okraje", "samostatná suchá handrička", "bez preneseného filmu"],
                ],
            },
            "steps": [
                "Zabezpečte priestor, parapet a bezpečný prístup.",
                "Odstráňte suchý prach z rámu, tesnenia a koľajničiek.",
                "Umyte rám vhodným prostriedkom a čistou handričkou.",
                "Sklo navlhčite v malej zóne a uvoľnite nečistotu.",
                "Stiahnite vodu zhora nadol a gumu priebežne utierajte.",
                "Okraje dosušte samostatnou čistou handričkou.",
                "Po úplnom vyschnutí skontrolujte obe strany v bočnom svetle.",
            ],
            "checks": [
                ["Náradie", "Je stierka nepoškodená a handrička na sklo bez mastnoty?"],
                ["Tempo", "Dokážete zónu umyť a stiahnuť skôr, než roztok zaschne?"],
                ["Bezpečnosť", "Je vonkajšia strana dostupná bez improvizovaného lezenia?"],
            ],
            "expert": [
                "Šmuhu tvorí nerovnomerný zvyšok po odparení vody: môže ísť o čistiaci film, mastnotu, minerály alebo nečistotu prenesenú náradím. Pracovný postup preto kontroluje čistotu pomôcok, množstvo výrobku a rýchlosť odparovania.",
                "Pri domácich čističoch rešpektujte etiketu, správne riedenie a kompatibilitu povrchu. EPA odporúča výrobky nemiešať a počas čistenia vetrať; chlórové výrobky sa nikdy nekombinujú s kyselinami ani inými čističmi.",
            ],
            "sources": [["US EPA: bezpečné používanie čistiacich výrobkov", EPA_CLEANING]],
            "commerce": CLEANING_COMMERCE,
            "faq": [
                ["Mám umývať okná na priamom slnku?", "Radšej nie, pretože roztok môže zaschnúť skôr, než ho rovnomerne stiahnete."],
                ["Koľko čističa použiť?", "Podľa etikety. Nadbytok môže zanechať film a zhoršiť leštenie."],
                ["Prečo čistiť rám ako prvý?", "Prach a špinavá voda z rámu by po umytí znovu znečistili sklo."],
                ["Je potrebná stierka?", "Nie vždy, no na veľkej ploche pomáha rovnomerne odstrániť vodu, ak je čistá a nepoškodená."],
                ["Ako zistiť, na ktorej strane je šmuha?", "Jednu stranu leštite zvislo a druhú vodorovne alebo ich skontrolujte z rôznych uhlov."],
            ],
        },
        {
            "post_id": "1459",
            "title": "Ako umyť okná bez šmúh – Kompletný sprievodca a odpovede na časté otázky",
            "slug": "ako-umyt-okna-bez-smuh-kompletny-sprievodca-a-odpovede-na-caste-otazky",
            "short": "Diagnostická príručka podľa vzhľadu šmuhy: mastné kruhy, zvislé pásy, biele bodky, špinavé okraje a rozdiel medzi vnútornou a vonkajšou stranou.",
            "description": "Prečo zostávajú na oknách šmuhy a ako ich opraviť: mastný film, minerály, stierka, handrička, slnko, rámy a časté otázky.",
            "quick": "Mastné kruhy ukazujú skôr na film alebo špinavú handričku, opakovaný zvislý pás na poškodenú či znečistenú stierku a biele bodky na minerálny zvyšok. Pred ďalším celým umývaním otestujte malú plochu čistou vodou a novou handričkou, aby ste určili príčinu.",
            "intro": "Tento článok začína pri už vzniknutom probléme a funguje ako diagnostická FAQ. Lineárny postup od rámu po stierku je v samostatnom kompletnom návode, aby sa oba články neopakovali.",
            "focus": "rozpoznanie typu šmuhy, lokalizáciu strany, test čistej vody, chyby náradia, minerálny film, mastnotu a cielenú opravu",
            "boundary": "bežný pracovný postup prvého umytia vrátane prípravy rámu a systematického stierania celej plochy",
            "points": [
                "tvar a poloha šmuhy pomáhajú určiť príčinu pred ďalším čistením",
                "malý kontrolný test je účinnejší než pridanie ďalšieho výrobku na celé okno",
                "špinavá handrička a poškodená stierka môžu vytvoriť rovnaký problém opakovane",
                "minerálny film, mastnota a čistiaci zvyšok potrebujú odlišnú opravu",
            ],
            "sections": [
                (
                    "Mastné kruhy a dúhový film",
                    [
                        "Kruhové stopy často vznikajú, keď handrička prenáša mastnotu alebo je na skle priveľa výrobku. Dúhový odlesk môže ukazovať na tenký organický film. Skúste malú zónu čistou handričkou a minimom vhodného roztoku.",
                        "Nepoužívajte avivážou ošetrené mikrovlákno, pretože povrch môže zanechať film. Handričky na sklo perte oddelene od mastných kuchynských textílií a úplne ich vypláchajte.",
                    ],
                ),
                (
                    "Pás presne po stierke",
                    [
                        "Rovnaký pás v každom ťahu naznačuje nečistotu alebo poškodenie gumy. Stierku umyte, skontrolujte hranu proti svetlu a pri záreze ju vymeňte. Po každom ťahu ju utrite.",
                        "Ak pás vzniká iba na jednom mieste okna, môže byť na skle bod nečistoty alebo nerovnosť, ktorá stierku nadvihne. Miesto najprv lokálne dočistite bez ostrého kovového nástroja.",
                    ],
                ),
                (
                    "Biele bodky a mapy po kvapkách",
                    [
                        "Bodky po zaschnutých kvapkách môžu byť minerálny zvyšok z vody. Najprv skúste malú plochu prostriedkom vhodným na sklo a rám. Kyslý výrobok nesmie stekať na citlivý kameň, kov alebo poškodené tesnenie.",
                        "Nikdy nemiešajte ocot alebo iný kyslý čistič s chlórovým výrobkom. Ak je sklo špeciálne povrchovo upravené, riaďte sa návodom výrobcu okna; agresívny domáci pokus môže vrstvu poškodiť.",
                    ],
                ),
                (
                    "Ktorá strana je špinavá",
                    [
                        "Jednu stranu pri finálnom leštení veďte zvislo a druhú vodorovne. Smer šmuhy potom ukáže, kde opravu urobiť. Pomôže aj bočné svetlo a pohľad z tmavšieho interiéru.",
                        "Ak sú stopy iba pri okrajoch, problém môže prichádzať z rámu alebo z handričky používanej na obe plochy. Samostatný správny pracovný postup nájdete v článku <a href=\"/n/ako-umyt-okna-bez-smuh-kompletny-sprievodca\">ako umyť okná bez šmúh krok za krokom</a>.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Vzhľad", "Pravdepodobná príčina", "Kontrolný test"],
                "rows": [
                    ["Mastné kruhy", "film alebo handrička", "čisté mikrovlákno na malej zóne"],
                    ["Rovný pás", "hrana stierky", "kontrola gumy proti svetlu"],
                    ["Biele bodky", "minerálny zvyšok", "lokálny kompatibilný test"],
                    ["Špinavé okraje", "rám alebo spoločná handrička", "oddelené náradie"],
                ],
            },
            "steps": [
                "Nechajte okno úplne vyschnúť a pozrite sa v bočnom svetle.",
                "Určte stranu, tvar, smer a polohu každej stopy.",
                "Skontrolujte čistotu handričiek a hranu stierky.",
                "Na malej ploche skúste čistú vodu a nové mikrovlákno.",
                "Podľa reakcie zvoľte riešenie pre film alebo minerály.",
                "Opravte iba postihnutú stranu a zónu.",
                "Po vyschnutí znovu skontrolujte bez pridávania ďalších vrstiev.",
            ],
            "checks": [
                ["Tvar", "Je stopa kruhová, rovná po stierke alebo bodkovaná po kvapkách?"],
                ["Strana", "Viete určiť, či je na vnútornej alebo vonkajšej ploche?"],
                ["Test", "Zmenila sa po čistej vode bez ďalšieho čističa?"],
            ],
            "expert": [
                "Diagnostika využíva morfológiu zvyšku. Plynulý film kopíruje pohyb handričky, pás kopíruje kontaktnú hranu stierky a bodky kopírujú kvapky. Tento vzor pomáha oddeliť nástroj, chémiu a vodu bez laboratórneho merania.",
                "Bezpečnosť čistiaceho zásahu závisí od materiálu rámu, povrchovej úpravy skla a kompatibility výrobku. Etiketa, malé skryté miesto, vetranie a zákaz miešania produktov majú prednosť pred univerzálnym domácim receptom.",
            ],
            "sources": [["US EPA: bezpečné používanie čistiacich výrobkov", EPA_CLEANING]],
            "commerce": CLEANING_COMMERCE,
            "faq": [
                ["Prečo vidím šmuhy až večer?", "Bočné svetlo zvýrazní tenký film, ktorý pri rozptýlenom dennom svetle nebol viditeľný."],
                ["Prečo stierka robí jeden pás?", "Na hrane môže byť nečistota, zárez alebo bod na skle, ktorý gumu nadvihuje."],
                ["Ako odstrániť biele bodky?", "Urobte lokálny test kompatibilného prostriedku a chráňte rám aj okolité materiály."],
                ["Môžem miešať ocot s bielidlom?", "Nie. Kyseliny a chlórové výrobky sa nikdy nemiešajú."],
                ["Prečo čistá handrička stále maže?", "Mohla byť praná s avivážou alebo mastnými textíliami a mať povlak vo vláknach."],
            ],
        },
        {
            "post_id": "1437",
            "title": "Koľko stojí jedno pranie? Kompletný sprievodca nákladmi na pranie",
            "slug": "kolko-stoji-jedno-pranie-kompletny-sprievodca-nakladmi-na-pranie",
            "short": "Náklad jedného prania vypočítate zo spotreby elektriny a vody konkrétneho programu, vlastných taríf, dávky výrobkov a primeraného podielu údržby.",
            "description": "Ako vypočítať cenu jedného prania bez zastaraných pevných cien: kWh, litre vody, tarify, dávka gélu, parfum, údržba a amortizácia.",
            "quick": "Spočítajte elektrinu programu, vodu, skutočne použitú dávku gélu a ďalších výrobkov a voliteľne odhad údržby či opotrebovania. Každú spotrebu násobte vlastnou aktuálnou jednotkovou cenou. Nepoužívajte univerzálnu sumu z internetu, pretože tarifa, program, práčka aj dávka sa líšia.",
            "intro": "Tento článok je kalkulačný model a vysvetľuje položky vzorca. Samostatný článok o nákladoch a úsporách používa výsledok na rozhodovanie, ktoré zmeny majú najväčší ročný efekt.",
            "focus": "presný vzorec, zdroj údajov o kWh a litroch, jednotkové ceny, náklad dávky výrobkov, údržbu, amortizáciu a porovnateľnosť cyklov",
            "boundary": "behaviorálne úspory, scenáre domácností a poradie krokov na znižovanie ročných výdavkov",
            "points": [
                "výpočet používa vaše aktuálne tarify a údaje konkrétneho programu, nie pevnú sumu",
                "energetický štítok opisuje štandardizovaný eco cyklus a nemusí zodpovedať každému programu",
                "náklad dávky sa počíta z ceny balenia a počtu reálnych dávok",
                "plný ekonomický model môže oddeliť prevádzku, údržbu a opotrebovanie",
            ],
            "sections": [
                (
                    "Elektrina a voda programu",
                    [
                        "Spotrebu elektriny pre konkrétny cyklus získate z dokumentácie, aplikácie spotrebiča alebo merania. Hodnotu v kWh vynásobte celkovou jednotkovou cenou elektriny podľa svojej faktúry, nie iba jednou tarifnou položkou.",
                        "Litre vody premeňte na kubické metre delením tisícom a vynásobte súčtom relevantného vodného a stočného. Ak domácnosť používa vlastný zdroj alebo inú schému, model upravte podľa skutočného nákladu.",
                    ],
                ),
                (
                    "Prací gél, parfum a ďalšie dávky",
                    [
                        "Jednotkový náklad výrobku vypočítajte ako cenu balenia delenú počtom reálnych dávok, nie marketingovým maximom, ak používate inú dávku. Pri tekutom výrobku možno cenu za mililiter násobiť odmeraným množstvom.",
                        "Každý doplnok pridajte ako samostatnú položku: parfum, odstraňovač škvŕn, zmäkčovadlo alebo hygienický prípravok. Tak uvidíte, ktorá časť mení celok a nebudete pripisovať všetko elektrine.",
                    ],
                ),
                (
                    "Údržba a opotrebovanie",
                    [
                        "Pre bežné porovnanie programov stačia variabilné náklady. Pre celkovú ekonomiku možno pridať ročný odhad servisu a čistenia delený počtom cyklov. Túto položku uvádzajte osobitne, pretože je neistá.",
                        "Amortizácia je obstarávacia cena mínus zostatková hodnota rozdelená odhadovaným počtom cyklov. Nie je to hotovostný výdavok pri každom praní, ale pomáha porovnať intenzitu používania a životnosť spotrebiča.",
                    ],
                ),
                (
                    "Ako porovnávať dva cykly férovo",
                    [
                        "Porovnávajte rovnakú alebo podobnú náplň a výslednú čistotu. Lacnejší krátky cyklus nie je úspora, ak musíte bielizeň prať znovu. Zapisujte hmotnosť náplne, program, teplotu, spotrebu a dávky.",
                        "Údaj na energetickom štítku sa viaže na štandardizovaný eco program a uvádza energiu na sto cyklov aj vodu na cyklus. Na iný program ho nepoužívajte ako presné meranie bez overenia.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Položka", "Vzorec", "Zdroj údaja"],
                "rows": [
                    ["Elektrina", "kWh × vaša cena za kWh", "manuál, aplikácia alebo meranie"],
                    ["Voda", "litre ÷ 1000 × cena za m³", "program a faktúra"],
                    ["Výrobok", "dávka × cena jednotky", "balenie a odmerka"],
                    ["Údržba", "ročný odhad ÷ počet cyklov", "vlastná evidencia"],
                ],
            },
            "steps": [
                "Vyberte konkrétny program a porovnateľnú náplň.",
                "Získajte jeho spotrebu elektriny a vody.",
                "Z faktúr doplňte aktuálne úplné jednotkové ceny.",
                "Odmerajte reálne dávky všetkých použitých výrobkov.",
                "Vypočítajte každú položku osobitne a potom ich sčítajte.",
                "Údržbu a amortizáciu uvádzajte oddelene ako odhad.",
                "Model aktualizujte pri zmene taríf, programu alebo dávkovania.",
            ],
            "checks": [
                ["Jednotky", "Sú kWh, litre, kubické metre a mililitre správne prepočítané?"],
                ["Tarifa", "Používate aktuálnu úplnú cenu z vlastnej faktúry?"],
                ["Výsledok", "Porovnávate cykly s podobnou náplňou a dosiahnutou čistotou?"],
            ],
            "expert": [
                "Energetický štítok EÚ uvádza pre práčky spotrebu energie na sto eco cyklov, vodu na jeden cyklus, kapacitu, trvanie a hluk. Ide o štandardizované údaje vhodné na porovnanie modelov, nie automaticky o presnú spotrebu každého domáceho programu.",
                "Rozdelenie fixných a variabilných nákladov zabraňuje dvojitému započítaniu. Elektrina, voda a dávky sa menia s cyklom; obstarávacia cena a časť údržby sa alokujú podľa zvoleného ekonomického modelu.",
            ],
            "sources": [["Európska komisia: energetický štítok práčok", EU_WASHING]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Aká je univerzálna cena jedného prania?", "Neexistuje presná univerzálna suma; závisí od spotreby, taríf, dávok, programu a modelu nákladov."],
                ["Môžem použiť údaj zo štítku pre každý program?", "Nie. Štítok používa štandardizovaný eco program a ostatné cykly sa môžu líšiť."],
                ["Ako vypočítať cenu gélu na cyklus?", "Cenu za mililiter alebo dávku vynásobte skutočne použitým množstvom."],
                ["Patrí do výpočtu cena práčky?", "Voliteľne ako amortizácia, oddelene od variabilných prevádzkových nákladov."],
                ["Ako často výpočet aktualizovať?", "Pri zmene taríf, dávkovania, programu, spotrebiča alebo cien výrobkov."],
            ],
        },
        {
            "post_id": "1375",
            "title": "Koľko stojí jedno pranie? Kompletný sprievodca nákladmi a úsporami",
            "slug": "kolko-stoji-jedno-pranie-kompletny-sprievodca-nakladmi-a-usporami",
            "short": "Po výpočte ceny jedného cyklu hľadajte úsporu v plnej primeranej náplni, účinnom programe, presnom dávkovaní, sušení a prevencii opakovaného prania.",
            "description": "Ako znížiť náklady na pranie bez zhoršenia výsledku: scenáre domácností, plnosť bubna, teplota, dávkovanie, opakované cykly a ročná úspora.",
            "quick": "Najväčšiu úsporu často neprinesie najkratší program, ale menej opakovaných cyklov, primerane naplnený bubon, presná dávka a vhodná teplota. Najprv si vypočítajte vlastný základný cyklus, potom meníte jednu premennú a ročný efekt počítajte ako úsporu na cyklus krát počet cyklov.",
            "intro": "Tento článok používa ekonomický model na rozhodovanie a úspory. Samostatný kalkulačný sprievodca podrobne vysvetľuje, ako získať kWh, litre, tarify a náklad každej dávky.",
            "focus": "poradie úsporných opatrení, scenáre rôznych domácností, primeranú náplň, teplotu, dávkovanie, opakované pranie a ročný efekt",
            "boundary": "detailný výpočet každej nákladovej položky, jednotiek, amortizácie a zdrojov údajov o spotrebe",
            "points": [
                "úspora je platná iba vtedy, keď bielizeň dosiahne potrebnú čistotu na prvýkrát",
                "ročný efekt závisí od počtu cyklov, preto domácnosti nemajú rovnaké priority",
                "primeraná plná náplň rozkladá fixnú spotrebu cyklu na viac textilu",
                "predávkovanie výrobkov zvyšuje náklad a môže vyvolať ďalší oplach alebo pranie",
            ],
            "sections": [
                (
                    "Najprv odstráňte opakované pranie",
                    [
                        "Cyklus, ktorý sa musí zopakovať pre zápach, škvrnu alebo zvyšky, zdvojnásobuje viacero položiek naraz. Pred úsporou teploty skontrolujte triedenie, predčistenie škvŕn, vhodný gél, náplň a sušenie.",
                        "Zapisujte dôvod opakovania. Ak ide o škvrny, zlepšite predčistenie; pri pachu riešte čas vo vlhku a práčku; pri mapách dávku a oplach. Každá príčina má inú ekonomickú páku.",
                    ],
                ),
                (
                    "Primeraná náplň a frekvencia",
                    [
                        "Časté poloprázdne cykly zvyšujú náklad na kilogram textilu. Zbierajte podobné kusy do primeranej náplne, no nečakajte tak dlho, aby mokré alebo organicky znečistené textílie zatuchli.",
                        "Preplnenie zasa znižuje mechaniku a oplach a môže vytvoriť opakovaný cyklus. Praktickým cieľom je náplň v rozsahu programu s priestorom na pohyb, nie maximálne natlačený bubon.",
                    ],
                ),
                (
                    "Teplota a program podľa skutočnej potreby",
                    [
                        "Nižšia teplota môže znížiť spotrebu energie, ak detergent a program zvládnu znečistenie a štítok ju povoľuje. Hygienicky náročná odolná náplň však môže potrebovať iný režim. Úspora nesmie ignorovať funkciu cyklu.",
                        "Najkratší program nemusí byť najlacnejší ani najúčinnejší. Skontrolujte údaje spotrebiča a výsledok. Eco program môže trvať dlhšie, pretože používa odlišnú kombináciu času, vody a teploty.",
                    ],
                ),
                (
                    "Dávkovanie a ročný scenár",
                    [
                        "Odmerku používajte podľa tvrdosti vody, náplne a znečistenia. Malé pravidelné predávkovanie sa za rok násobí počtom cyklov a môže pridať náklad na oplach. Rovnaké platí pre všetky doplnkové výrobky.",
                        "Ročnú úsporu vypočítajte ako rozdiel nákladov na jeden porovnateľný cyklus krát realistický počet takých cyklov. Pri výpočte použite model z článku <a href=\"/n/kolko-stoji-jedno-pranie-kompletny-sprievodca-nakladmi-na-pranie\">koľko stojí jedno pranie</a>.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Opatrenie", "Možný prínos", "Podmienka"],
                "rows": [
                    ["Menej opakovaní", "šetrí celý cyklus", "správny výsledok na prvýkrát"],
                    ["Primeraná náplň", "nižší náklad na kilogram", "dostatok pohybu"],
                    ["Nižšia teplota", "menej ohrevu", "štítok a účinnosť"],
                    ["Presná dávka", "menej výrobku a zvyškov", "znalosť vody a náplne"],
                ],
            },
            "steps": [
                "Vypočítajte vlastný základný cyklus s aktuálnymi tarifami.",
                "Zapíšte dôvody opakovaných praní a oplachov.",
                "Vyberte jednu zmenu s najväčšou pravdepodobnou pákou.",
                "Otestujte ju na porovnateľnej náplni bez zmeny ostatných podmienok.",
                "Overte čistotu, funkciu textilu a potrebu opakovania.",
                "Rozdiel na cyklus vynásobte realistickou ročnou frekvenciou.",
                "Opatrenie ponechajte iba vtedy, keď kvalita výsledku neklesla.",
            ],
            "checks": [
                ["Kvalita", "Dosiahla úspornejšia voľba rovnakú potrebnú čistotu a hygienu?"],
                ["Frekvencia", "Koľkokrát ročne sa daný typ cyklu skutočne opakuje?"],
                ["Vedľajší náklad", "Nevznikol ďalší oplach, predčistenie alebo opakované pranie?"],
            ],
            "expert": [
                "Analýza citlivosti mení vždy jednu vstupnú premennú a sleduje jej vplyv na výsledok. Pri praní pomáha oddeliť tarifu, počet cyklov, teplotu, náplň a dávku, aby sa úspora nepripísala nesprávnemu kroku.",
                "Štandardizovaný energetický štítok umožňuje porovnávať spotrebiče na eco programe. Domáca optimalizácia však potrebuje skutočnú frekvenciu a náplne používateľa; ročný scenár jednej osoby sa líši od rodiny s denným praním.",
            ],
            "sources": [["Európska komisia: energetický štítok práčok", EU_WASHING], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Je najkratší program najlacnejší?", "Nie nevyhnutne. Porovnajte údaje spotrebiča aj to, či dosiahne výsledok bez opakovania."],
                ["Oplatí sa prať vždy na nízkej teplote?", "Iba ak ju povoľuje štítok a cyklus zvládne znečistenie a hygienický cieľ."],
                ["Ako vypočítať ročnú úsporu?", "Rozdiel nákladov na cyklus vynásobte počtom porovnateľných cyklov za rok."],
                ["Je plný bubon vždy úsporný?", "Primeraná náplň áno, preplnenie však zhoršuje pranie a môže vyvolať opakovanie."],
                ["Kde začať?", "Najprv odstráňte opakované cykly a predávkovanie, potom testujte teplotu a program."],
            ],
        },
    ]
)

CONFIGS.extend(
    [
        {
            "post_id": "1185",
            "title": "Najlepší parfum do prania",
            "slug": "najlepsi-parfum-do-prania",
            "short": "Najlepší parfum do prania nie je jedna univerzálna vôňa; vyberá sa podľa intenzity, rodiny tónov, textilu, spôsobu sušenia a citlivosti domácnosti.",
            "description": "Ako vybrať najlepší parfum do prania pomocou rozhodovacej matice: intenzita, vonná rodina, textil, sušenie, vzorky a citlivosť.",
            "quick": "Najlepší parfum do prania je ten, ktorý vám vonia na suchej bielizni, neprekáža pri dlhom nosení a funguje pri vašej dávke, vode a sušení. Začnite rodinou vône, porovnajte vzorky na rovnakom textile a výsledok posúďte po niekoľkých hodinách, nie iba z fľaše.",
            "intro": "Tento článok je objektívna nákupná a používateľská matica. Samostatný text o našom favoritovi vysvetľuje, prečo je čuch osobný a ako urobiť slepý domáci test bez ovplyvnenia názvom či prvým dojmom.",
            "focus": "výber podľa rodiny vône, intenzity, materiálu, sušenia, citlivosti, veľkosti domácnosti a skúšania vzoriek",
            "boundary": "subjektívny senzorický test osobného favorita a technická definícia parfumovej kompozície",
            "points": [
                "vôňa z fľaše nepredpovedá presne výsledok po praní a vysušení",
                "najlepší výber vzniká porovnaním rovnakej dávky na rovnakom textile",
                "intenzita sa prispôsobuje domácnosti, nie iba osobnému prvému dojmu",
                "vzorka znižuje riziko, že veľké balenie nebude fungovať v bežnej rutine",
            ],
            "sections": [
                (
                    "Najprv vyberte rodinu vône",
                    [
                        "Svieže citrusové a čisté tóny pôsobia ľahšie, kvetinové môžu byť mäkké alebo výrazné, drevité a orientálne kompozície bývajú plnšie. Názov rodiny je orientácia, nie presný opis intenzity alebo výdrže.",
                        "Spíšte si dve vône, ktoré radi nosíte alebo používate doma, a dve, ktoré vám prekážajú. Hľadajte spoločné tóny. Tak zúžite výber lepšie než podľa fotografie balenia alebo označenia bestseller.",
                    ],
                ),
                (
                    "Intenzita pre oblečenie a domácnosť",
                    [
                        "Pracovná košeľa, posteľná bielizeň a športové oblečenie sa používajú v odlišnej vzdialenosti od tváre a iný čas. Výrazná vôňa na uteráku môže byť príjemná, no na šále alebo pyžame rušivá.",
                        "Zohľadnite deti, domáce zvieratá, alergiu, astmu a citlivosť členov domácnosti. Pri zdravotných ťažkostiach vôňu nepoužívajte ako skúšku tolerancie a riaďte sa odporúčaním zdravotníka.",
                    ],
                ),
                (
                    "Vzorka a porovnateľný test",
                    [
                        "Dve vzorky skúšajte na podobných bavlnených kusoch, pri rovnakej dávke gélu, programe a sušení. Označte ich až po vysušení alebo požiadajte inú osobu, aby kódy zamiešala. Znížite vplyv názvu a očakávania.",
                        "Vôňu posúďte po vysušení, po jednom dni v skrini a pri nosení. Sledujte príjemnosť, intenzitu, vývoj a to, či sa nemieša s pachom nedostatočne vypranej bielizne.",
                    ],
                ),
                (
                    "Ako zahrnúť vodu a sušenie",
                    [
                        "Tvrdosť vody, množstvo gélu, plákanie a materiál menia výsledok. Ak porovnávate vône, nemeníte súčasne tieto premenné. Inak neviete, či rozdiel vytvorila kompozícia alebo proces.",
                        "Sušička a voľné sušenie môžu tú istú vôňu posunúť. Pri sušičke používajte iba postup určený výrobcom a výsledok hodnotte po vychladnutí. Presušenie môže znížiť príjemnosť aj funkciu textilu.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Preferencia", "Skúste najprv", "Pozor na"],
                "rows": [
                    ["Ľahká čistota", "svieže a citrusové", "príliš ostrý vrch"],
                    ["Mäkká elegancia", "kvetinové a púdrové", "sladkosť pri pyžame"],
                    ["Hĺbka", "drevité a orientálne", "intenzitu v malom priestore"],
                    ["Neistý výber", "sadu vzoriek", "porovnanie z fľaše"],
                ],
            },
            "steps": [
                "Spíšte obľúbené a neobľúbené vonné tóny.",
                "Vyberte dve odlišné rodiny a malé vzorky.",
                "Pripravte porovnateľné kusy a rovnaký prací proces.",
                "Použite spodnú odporúčanú dávku podľa etikety.",
                "Vzorky označte kódom a nechajte úplne vyschnúť.",
                "Hodnoťte po vysušení, po skladovaní a pri nosení.",
                "Až potom vyberte vôňu a dolaďte dávku po malých krokoch.",
            ],
            "checks": [
                ["Rodina", "Viete pomenovať tóny, ktoré chcete aj tie, ktorým sa vyhýbate?"],
                ["Porovnanie", "Mali obe vzorky rovnaký textil, dávku, program a sušenie?"],
                ["Tolerancia", "Je vôňa príjemná aj po hodinách a všetkým členom domácnosti?"],
            ],
            "expert": [
                "Čuchová adaptácia znižuje vnímanie vône pri dlhšom kontakte. Používateľ potom môže dávku zvyšovať, hoci človek, ktorý vstúpi do priestoru alebo si odev oblečie prvýkrát, ju vníma intenzívnejšie. Preto pomáhajú prestávky a zaslepené porovnanie.",
                "IFRA štandardy riešia bezpečné použitie vybraných vonných surovín podľa kategórie výrobku. Spotrebiteľ má používať hotový parfum do prania podľa jeho etikety a nemá nahrádzať formulovaný výrobok samostatnou vonnou surovinou.",
            ],
            "sources": [["IFRA Code of Practice", IFRA], ["Laundry malodour and textile microbiome", LAUNDRY_MALODOUR]],
            "commerce": {**FRAGRANCE_COMMERCE, "product_title": "Sada vzoriek parfumov do prania", "product_body": "Viac vôní na porovnateľný domáci test pred výberom väčšieho balenia.", "product_href": "/p-249/sada-vsetkych-6-vzoriek-po-1ks"},
            "faq": [
                ["Ktorý parfum do prania je najlepší?", "Ten, ktorý vyhovuje vašej vôni, dávke, textilu, sušeniu a citlivosti domácnosti."],
                ["Mám vyberať podľa vône z fľaše?", "Nie iba podľa nej. Výsledok na suchej bielizni sa môže výrazne líšiť."],
                ["Ako porovnať dve vône?", "Použite rovnaký textil, program, dávku a sušenie a hodnotenie rozložte v čase."],
                ["Je výraznejšia vôňa kvalitnejšia?", "Nie. Intenzita, príjemnosť, vyváženosť a vhodnosť pre použitie sú odlišné vlastnosti."],
                ["Prečo začať vzorkou?", "Overíte výsledok vo vlastnej vode a rutine bez zbytočného veľkého balenia."],
            ],
        },
        {
            "post_id": "1193",
            "title": "Najlepší parfum do prania? Záleží na vkuse – no náš favorit je jasný",
            "slug": "najlepsi-parfum-do-prania-zalezi-na-vkuse-no-nas-favorit-je-jasny",
            "short": "Osobný favorit sa má overiť slepým testom na suchej bielizni; názov, obal a prvý nádych z fľaše ľahko ovplyvnia očakávanie.",
            "description": "Ako si nájsť osobný parfum do prania: slepý senzorický test, čuchová adaptácia, hodnotenie v čase a rozdiel medzi favoritom a univerzálnym víťazom.",
            "quick": "Favorit je osobná voľba, nie objektívny víťaz pre každého. Vyskúšajte dve alebo tri vône pod kódom, na rovnakom textile a pri rovnakej dávke. Hodnoťte prvý dojem, stav po vysušení, po dni v skrini a pri nosení. Až potom si prečítajte názvy.",
            "intro": "Tento článok vysvetľuje senzorické testovanie a subjektívny vkus. Ak potrebujete najprv zúžiť výber podľa rodiny, intenzity, textilu a domácnosti, použite samostatnú rozhodovaciu maticu.",
            "focus": "osobný favorit, slepý test, čuchovú adaptáciu, očakávanie podľa názvu, hodnotenie v čase a spoločnú voľbu domácnosti",
            "boundary": "systematický nákupný výber medzi vonnými rodinami a technické vysvetlenie zloženia parfumu",
            "points": [
                "slovo najlepší v tomto článku znamená osobný favorit, nie univerzálne poradie",
                "zaslepenie znižuje vplyv názvu, farby obalu a odporúčania iných ľudí",
                "jednu vôňu hodnotíme v niekoľkých časoch a na reálnom odeve",
                "favorit domácnosti musí byť prijateľný aj pre citlivejších členov",
            ],
            "sections": [
                (
                    "Prečo nás ovplyvňuje názov a obal",
                    [
                        "Názov môže vyvolať predstavu čistoty, luxusu, kvetov alebo dovolenky ešte pred privoňaním. Farba a opis tónov nastavujú očakávanie. To nie je chyba, ale pri porovnaní je užitočné tento vplyv na chvíľu odstrániť.",
                        "Vzorky označte náhodnými písmenami a poradie nepoznajte. Ak test pripraví iný člen domácnosti, hodnotenie sa viac sústredí na reálny vnem než na značku alebo predchádzajúci názor.",
                    ],
                ),
                (
                    "Štyri chvíle jedného hodnotenia",
                    [
                        "Prvý nádych po vysušení ukáže vrchný dojem. Po niekoľkých hodinách sa zvýraznia iné tóny. Deň v skrini preverí výdrž a pri nosení zistíte, či vôňa neprekáža pri kontakte s telom a teplom.",
                        "Každú fázu ohodnoťte osobitne: príjemnosť, intenzitu, čistotu dojmu a rušivosť. Jedna vysoká známka po otvorení práčky nemá automaticky vyhrať nad vôňou, ktorá je vyvážená celý deň.",
                    ],
                ),
                (
                    "Čuchová adaptácia a prestávky",
                    [
                        "Pri opakovanom čuchaní sa citlivosť na rovnaký podnet znižuje. Medzi vzorkami si dajte prestávku, choďte na čerstvý vzduch a netestujte veľa vôní naraz. Inak sa rozdiely zlievajú.",
                        "Nezvyšujte dávku len preto, že vôňu po niekoľkých minútach registrujete menej. Požiadajte o názor človeka, ktorý do miestnosti práve prišiel, alebo hodnotenie zopakujte na ďalší deň.",
                    ],
                ),
                (
                    "Ako vybrať spoločný favorit",
                    [
                        "Každý člen domácnosti nech hodnotí samostatne a bez presviedčania. Vôňa s najvyšším priemerom nemusí vyhrať, ak jednému človeku spôsobuje nevoľnosť alebo bolesť hlavy. V takom prípade znížte dávku alebo zvoľte inú kompozíciu.",
                        "Náš favorit môže byť konkrétnym odporúčaním, nie zárukou rovnakého výsledku pre vás. Praktický výber rodiny a vzoriek rozoberá článok <a href=\"/n/najlepsi-parfum-do-prania\">najlepší parfum do prania</a>.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Čas", "Čo hodnotiť", "Typická chyba"],
                "rows": [
                    ["Po vysušení", "prvý dojem", "hodnotiť horúci textil"],
                    ["Po pár hodinách", "vývoj tónov", "privoňať bez prestávky"],
                    ["Po dni v skrini", "výdrž", "meniť dávku medzi vzorkami"],
                    ["Pri nosení", "rušivosť", "ignorovať citlivosť"],
                ],
            },
            "steps": [
                "Vyberte najviac tri vzorky a rovnaké testovacie kusy.",
                "Požiadajte inú osobu o náhodné označenie kódmi.",
                "Použite rovnaký program, gél, dávku a sušenie.",
                "Hodnoťte po vysušení bez poznania názvu.",
                "Zopakujte hodnotenie po hodinách, skladovaní a pri nosení.",
                "Porovnajte poznámky členov domácnosti a citlivosť.",
                "Odhaľte názvy až po uzavretí hodnotenia.",
            ],
            "checks": [
                ["Zaslepenie", "Nevedeli hodnotitelia, ktorá značka alebo vôňa je v konkrétnej vzorke?"],
                ["Rovnaké podmienky", "Mali všetky vzorky rovnakú dávku, textil a sušenie?"],
                ["Čas", "Hodnotili ste aj vývoj po hodinách a pri nosení?"],
            ],
            "expert": [
                "Senzorické hodnotenie pracuje s kontrolou poradia, zaslepením a porovnateľnými vzorkami, pretože očakávanie a adaptácia menia vnem. Domáci test nie je laboratórium, no tieto jednoduché prvky výrazne zlepšia rozhodnutie.",
                "Bezpečné použitie hotového výrobku ostáva nadradené preferencii. Ak vôňa vyvoláva zdravotné ťažkosti, test ukončite, vyvetrajte a ďalší postup konzultujte podľa závažnosti s lekárom.",
            ],
            "sources": [["IFRA Code of Practice", IFRA], ["IFRA: ako vzniká parfumová kompozícia", "https://ifrafragrance.org/about-fragrance/how-is-fragrance-made"]],
            "commerce": {**FRAGRANCE_COMMERCE, "product_title": "Sada vzoriek parfumov do prania", "product_body": "Praktický základ slepého porovnania viacerých vôní na rovnakom textile.", "product_href": "/p-249/sada-vsetkych-6-vzoriek-po-1ks"},
            "faq": [
                ["Existuje objektívne najlepší parfum do prania?", "Nie pre každého. Preferencia, dávka, textil a citlivosť sa medzi ľuďmi líšia."],
                ["Prečo robiť slepý test?", "Znižuje vplyv názvu, obalu, značky a odporúčania na hodnotenie."],
                ["Koľko vôní porovnať naraz?", "Ideálne dve až tri, aby sa vnem nezahltil a zostal čas na prestávky."],
                ["Kedy je výsledok najspoľahlivejší?", "Po opakovanom hodnotení na suchej bielizni, po skladovaní a pri nosení."],
                ["Čo ak vôňa niekomu spôsobuje bolesť hlavy?", "Prestaňte ju používať, vyvetrajte a podľa závažnosti riešte zdravotné ťažkosti s lekárom."],
            ],
        },
        {
            "post_id": "1576",
            "title": "Parfum do prania – Čo to je a ako funguje",
            "slug": "parfum-do-prania-co-to-je-a-ako-funguje",
            "short": "Parfum do prania je formulovaný vonný výrobok určený na použitie v pracom procese; nie je detergent, aviváž ani parfum na pokožku.",
            "description": "Slovníkové vysvetlenie parfumu do prania: definícia, funkcia, parfumová kompozícia, nosič, dávkovanie, rozdiel od gélu a aviváže.",
            "quick": "Parfum do prania je hotová formulácia určená na prevoňanie textilu počas prania. Nečistí namiesto pracieho gélu a automaticky nezmäkčuje ako aviváž. Používa sa podľa etikety v určenej priehradke alebo fáze cyklu a jeho koncentrát sa neleje priamo na oblečenie.",
            "intro": "Toto je slovníkové heslo: presne oddeľuje pojmy, zložky a funkcie. Praktický prvý cyklus pre začiatočníka a úplný výberový sprievodca sú samostatné články s odlišným cieľom.",
            "focus": "definíciu výrobku, parfumovú kompozíciu, nosič, väzbu na textil, rozdiel od detergentu a aviváže a základné bezpečné použitie",
            "boundary": "podrobný nákupný výber, prvé praktické pranie a riešenie konkrétnych problémov s dávkou alebo zápachom",
            "points": [
                "parfum do prania je kategória hotového výrobku, nie jedna vonná surovina",
                "čistenie zabezpečuje detergent a vôňa sa pridáva až k čistému základu",
                "spôsob použitia určuje formulácia a etiketa konkrétneho výrobku",
                "parfum na pokožku, interiérový sprej a parfum do prania nie sú zameniteľné",
            ],
            "sections": [
                (
                    "Presná definícia parfumu do prania",
                    [
                        "Ide o spotrebiteľský výrobok, ktorý obsahuje parfumovú kompozíciu v nosiči a je navrhnutý pre konkrétnu fázu pracieho procesu. Kompozícia môže obsahovať prírodné aj syntetické vonné suroviny a pomocné zložky potrebné pre stabilitu.",
                        "Slovo parfum tu opisuje funkciu vône, nie klasifikáciu EdP alebo EdT používanú pri osobných parfumoch. Percentá a dávkovanie nemožno prenášať medzi kategóriami výrobkov bez údajov výrobcu.",
                    ],
                ),
                (
                    "Ako sa vôňa dostáva na textil",
                    [
                        "Počas cyklu sa formulácia rozptýli vo vode a časť vonných zložiek sa zachytí na vláknach alebo v nich. Výsledok mení materiál, detergent, tvrdosť vody, objem náplne, plákanie a sušenie.",
                        "Nie všetky zložky sa správajú rovnako. Ľahšie tóny sa prejavia rýchlo a odparujú skôr, ťažšie môžu zostať dlhšie. Vonná pyramída je opis vývoja, nie záruka presného času na každom textile.",
                    ],
                ),
                (
                    "Rozdiel oproti gélu a aviváži",
                    [
                        "Prací gél obsahuje povrchovo aktívne a ďalšie pracie zložky na odstránenie nečistôt. Aviváž mení povrch a dotyk vlákien. Parfum do prania má hlavný cieľ prevoňať; konkrétny výrobok môže mať ďalšie vlastnosti, ktoré treba overiť na etikete.",
                        "Produkty nemožno zamieňať podľa vône alebo vzhľadu. Koncentrát určený do pracieho procesu sa nedáva na pokožku a osobný parfum sa neleje do práčky. Každá kategória má odlišnú expozíciu a technické požiadavky.",
                    ],
                ),
                (
                    "Čo parfum nevyrieši",
                    [
                        "Nevyrieši nedostatočne vypraný pot, pleseň, zatuchnutú práčku, vlhkú skriňu ani zvyšky gélu. Pri pachu najprv odstráňte zdroj. Až čistá a úplne suchá bielizeň je vhodný základ na hodnotenie kompozície.",
                        "Ak vôňa po cykle slabne, postupujte systematicky: skontrolujte dávku, priehradku, náplň, materiál, plákanie a sušenie. Nezvyšujte množstvo bez toho, aby ste vedeli, kde sa proces odchýlil.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Výrobok", "Hlavná funkcia", "Nie je určený na"],
                "rows": [
                    ["Prací gél", "odstránenie nečistôt", "samostatné prevoňanie"],
                    ["Aviváž", "úprava povrchu vlákna", "pranie funkčného textilu bez kontroly"],
                    ["Parfum do prania", "vôňa textilu", "nahradenie detergentu"],
                    ["Osobný parfum", "vôňa pokožky", "nalievanie do práčky"],
                ],
            },
            "steps": [
                "Prečítajte názov kategórie, účel a etiketu výrobku.",
                "Overte, do ktorej priehradky alebo fázy cyklu patrí.",
                "Pripravte čistú, roztriedenú a primeranú náplň.",
                "Použite detergent podľa znečistenia a vody.",
                "Parfum odmerajte podľa etikety bez priameho kontaktu s látkou.",
                "Po cykle bielizeň úplne vysušte a vyvetrajte.",
                "Výsledok posúďte oddelene ako čistotu, funkciu a vôňu.",
            ],
            "checks": [
                ["Kategória", "Je výrobok výslovne určený do pracieho procesu?"],
                ["Funkcia", "Používate samostatný vhodný detergent na samotné čistenie?"],
                ["Aplikácia", "Dodržiavate priehradku, dávku a bezpečnostné pokyny na etikete?"],
            ],
            "expert": [
                "Parfumová kompozícia je zmes mnohých surovín navrhnutá pre konkrétny výrobkový základ a spôsob expozície. Pôvod suroviny sám o sebe neurčuje bezpečnosť ani stabilitu; hodnotí sa konkrétna látka, koncentrácia a kategória použitia.",
                "IFRA Code of Practice poskytuje systém štandardov pre bezpečné použitie vonných surovín. Spotrebiteľ však pracuje s etiketou hotového výrobku, pretože z nej vyplýva určené použitie a dávkovanie konkrétnej formulácie.",
            ],
            "sources": [["IFRA Code of Practice", IFRA], ["IFRA: ako vzniká parfumová kompozícia", "https://ifrafragrance.org/about-fragrance/how-is-fragrance-made"]],
            "commerce": FRAGRANCE_COMMERCE,
            "faq": [
                ["Je parfum do prania prací prostriedok?", "Nie. Hlavnou úlohou je vôňa; na odstránenie nečistôt použite vhodný detergent."],
                ["Je to aviváž?", "Nie automaticky. Aviváž upravuje povrch vlákna, zatiaľ čo parfum do prania má primárne vonnú funkciu."],
                ["Môžem ho naliať priamo na oblečenie?", "Nie. Použite iba postup a priehradku uvedenú na etikete."],
                ["Môžem použiť osobný parfum v práčke?", "Nie. Nie je formulovaný ani určený pre prací proces."],
                ["Prečo vôňa na rôznych látkach vydrží inak?", "Materiál, plákanie, sušenie a prchavosť zložiek menia zachytenie aj uvoľňovanie vône."],
            ],
        },
        {
            "post_id": "1186",
            "title": "🌸 Čo je parfum do prania a ako funguje?",
            "slug": "co-je-parfum-do-prania-a-ako-funguje",
            "short": "Praktický úvod pre prvé použitie parfumu do prania: čo pripraviť, kam ho naliať, ako dávkovať a ako vyhodnotiť výsledok.",
            "description": "Parfum do prania pre začiatočníkov: prvý cyklus, priehradka, dávka, prací gél, vhodná náplň, sušenie a najčastejšie chyby.",
            "quick": "Pri prvom použití vyberte bežnú bavlnenú náplň, skontrolujte etiketu výrobku, použite spodnú odporúčanú dávku v určenej priehradke a samostatne pridajte prací gél. Po cykle bielizeň úplne vysušte a až potom rozhodnite, či chcete vôňu nabudúce zosilniť alebo zjemniť.",
            "intro": "Tento článok je praktický prvý kontakt pre človeka, ktorý parfum do prania ešte nepoužíval. Slovníkové heslo vysvetľuje definície a úplný sprievodca rieši celý výber, pokročilé kombinácie a diagnostiku.",
            "focus": "prvý testovací cyklus, vhodnú náplň, priehradku, dávku, kombináciu s gélom, sušenie a bezpečné dolaďovanie intenzity",
            "boundary": "chemickú definíciu parfumovej kompozície a rozsiahly výber medzi materiálmi, vôňami a problémovými situáciami",
            "points": [
                "prvý cyklus má byť jednoduchý a porovnateľný, nie plný citlivých a problémových kusov",
                "spodná odporúčaná dávka je bezpečnejší začiatok než pokus o maximálnu intenzitu",
                "prací gél čistí a parfum sa pridáva ako samostatná vonná vrstva",
                "výsledok sa hodnotí na suchej bielizni a po niekoľkých hodinách",
            ],
            "sections": [
                (
                    "Čo pripraviť na prvý cyklus",
                    [
                        "Vyberte menšiu bežnú náplň s podobnými bavlnenými kusmi, ktorých štítok poznáte. Nezačínajte vlnou, hodvábom, membránou, športovým mikrovláknom ani odevom, ktorý už zapácha po predchádzajúcom praní.",
                        "Pripravte prací gél, parfum, odmerku a etikety oboch výrobkov. Skontrolujte tvrdosť vody a nepreplňte bubon. Tak bude prvý výsledok čitateľný a ďalšia úprava dávky zmysluplná.",
                    ],
                ),
                (
                    "Kam parfum patrí",
                    [
                        "Použite priehradku alebo spôsob aplikácie uvedený na konkrétnom výrobku. Nespoliehajte sa na farbu priehradky alebo postup inej značky. Koncentrát nelejte na suchý textil a nemiešajte ho v odmerke s detergentom bez pokynu výrobcu.",
                        "Prací gél patrí do svojej určenej časti a parfum má vlastnú funkciu. Ak zásobník zadržiava vodu alebo je zanesený, najprv ho vyčistite. Nesprávne vyplavenie môže zmeniť dávku aj čas kontaktu.",
                    ],
                ),
                (
                    "Ako nastaviť prvú dávku",
                    [
                        "Začnite spodnou hranicou etikety vzhľadom na objem náplne. Čuch pri nalievaní nie je vhodné meradlo, pretože koncentrát je omnoho intenzívnejší než výsledok po praní.",
                        "Pri ďalšom cykle meňte iba dávku parfumu a všetko ostatné ponechajte rovnaké. Malý krok ukáže smer bez zbytočného predávkovania. Ak je bielizeň klzká alebo mapovitá, najprv riešte oplach.",
                    ],
                ),
                (
                    "Prvé hodnotenie bez skreslenia",
                    [
                        "Po skončení náplň vyberte bez čakania a usušte rovnakým spôsobom ako zvyčajne. Horúci bubon alebo mokrá bielizeň uvoľňujú vôňu inak. Hodnoťte po vysušení, vychladnutí a krátkom skladovaní.",
                        "Sledujte čistotu, intenzitu, príjemnosť a rušivosť osobitne. Ak zostal pôvodný pach, ďalšia vôňa nie je riešenie. Najprv skontrolujte gél, program, práčku a sušenie.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Prvý krok", "Dobrá voľba", "Nevhodný začiatok"],
                "rows": [
                    ["Náplň", "podobná bavlna", "vlna a membrána"],
                    ["Dávka", "spodná odporúčaná", "maximum naslepo"],
                    ["Aplikácia", "podľa etikety", "priamo na textil"],
                    ["Hodnotenie", "suchý kus", "koncentrát vo fľaši"],
                ],
            },
            "steps": [
                "Vyberte jednoduchú podobnú náplň a skontrolujte štítky.",
                "Prečítajte etiketu gélu aj parfumu a určte priehradky.",
                "Naplňte bubon s dostatkom priestoru na pohyb.",
                "Odmerajte prací gél podľa vody a znečistenia.",
                "Pridajte spodnú odporúčanú dávku parfumu.",
                "Po cykle bielizeň okamžite a úplne vysušte.",
                "Zapíšte výsledok a pri ďalšom teste zmeňte iba jednu vec.",
            ],
            "checks": [
                ["Jednoduchosť", "Je prvá náplň bez citlivých materiálov a starého pachu?"],
                ["Priehradka", "Potvrdzuje etiketa presne miesto a spôsob použitia?"],
                ["Hodnotenie", "Je bielizeň pred posúdením úplne suchá a vychladnutá?"],
            ],
            "expert": [
                "Prvý cyklus je domáci kontrolovaný pokus. Keď sa naraz mení parfum, gél, program, teplota a sušenie, výsledok nemožno pripísať jednej príčine. Jednoduchý test znižuje spotrebu aj riziko nesprávneho záveru.",
                "Parfumové suroviny sa posudzujú podľa kategórie a expozície. Začiatočník má používať iba hotový výrobok určený do prania a rešpektovať jeho etiketu, nie experimentovať so samostatnými olejmi alebo osobným parfumom.",
            ],
            "sources": [["IFRA Code of Practice", IFRA], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
            "commerce": FRAGRANCE_COMMERCE,
            "faq": [
                ["Potrebujem aj prací gél?", "Áno. Parfum nenahrádza detergent určený na odstránenie nečistôt."],
                ["Kam parfum naliať?", "Do priehradky alebo spôsobom uvedeným na etikete konkrétneho výrobku."],
                ["Koľko použiť prvýkrát?", "Začnite spodnou odporúčanou dávkou vzhľadom na náplň."],
                ["Môžem ho naliať na tričko?", "Nie. Koncentrát nesmie ísť priamo na textil bez výslovného pokynu výrobcu."],
                ["Kedy zvýšiť dávku?", "Až po vyhodnotení suchej bielizne a po overení, že čistota, oplach a sušenie sú v poriadku."],
            ],
        },
        {
            "post_id": "1623",
            "title": "Parfum do prania - Kompletný sprievodca | Čo to je, ako funguje a ako ho používať",
            "slug": "parfum-do-prania-kompletny-sprievodca-co-to-je-ako-funguje-a-ako-ho-pouzivat",
            "short": "Kompletný sprievodca spája výber vône, dávkovanie, materiály, vodu, prací gél, sušenie, citlivosť a diagnostiku výsledku.",
            "description": "Kompletný sprievodca parfumom do prania: definícia, výber, dávkovanie, kombinovanie s gélom, materiály, tvrdá voda, sušenie a problémy.",
            "quick": "Parfum do prania vyberte podľa charakteru vône a citlivosti domácnosti, používajte podľa etikety spolu s vhodným detergentom a dávku dolaďujte na suchej bielizni. Pri probléme postupujte v poradí čistota, dávka, priehradka, náplň, plákanie, voda a sušenie; až potom meňte vôňu.",
            "intro": "Toto je hlavný rozcestníkový článok, ktorý spája celý proces a odkazuje na užšie návody. Slovník vysvetľuje definíciu a začiatočnícky článok prvý testovací cyklus, aby sa tu priestor využil na rozhodovanie medzi situáciami.",
            "focus": "celý životný cyklus výberu a použitia, od rodiny vône cez dávku a textil až po diagnostiku vody, oplachu a sušenia",
            "boundary": "jedinú slovníkovú definíciu alebo jeden prvý cyklus; tie sú podrobnejšie spracované v samostatných krátko zameraných návodoch",
            "points": [
                "najprv vyberte správnu kategóriu výrobku a až potom konkrétnu vôňu",
                "čistota, funkcia textilu a vôňa sú tri oddelené výsledky jedného cyklu",
                "dávku meníme až po kontrole náplne, vody, plákania a sušenia",
                "citlivé materiály a citliví ľudia majú prednosť pred intenzitou",
            ],
            "sections": [
                (
                    "Od výberu rodiny po vzorku",
                    [
                        "Začnite sviežou, kvetinovou, púdrovou, drevitou alebo orientálnou rodinou a zohľadnite, kde bude textil používaný. Posteľná bielizeň pri tvári potrebuje často jemnejšiu intenzitu než uterák v kúpeľni.",
                        "Vzorku otestujte na vlastnej vode a programe. Rozhodovacia matica je v článku <a href=\"/n/najlepsi-parfum-do-prania\">najlepší parfum do prania</a>. Výber z fľaše bez prania je iba orientačný.",
                    ],
                ),
                (
                    "Gél, dávka a priehradka",
                    [
                        "Detergent odstráni nečistoty a parfum pridá vonnú vrstvu. Používajte oddelené dávky a priehradky podľa etikiet. Pri zanesenom zásobníku sa výrobok môže vyplaviť v nesprávnom čase alebo zostať v priehradke.",
                        "Dávku určujú odporúčania výrobku a veľkosť náplne. Tvrdosť vody ovplyvňuje najmä prací proces a zvyšky; príliš veľa gélu alebo parfumu môže zhoršiť oplach a vytvoriť ťažký dojem.",
                    ],
                ),
                (
                    "Materiály a funkcia textilu",
                    [
                        "Bavlna, syntetika, mikrovlákno, membrána, vlna a hodváb sa správajú odlišne. Ošetrovací štítok je prvý filter. Pri športových vrstvách, uterákoch a čistiacom mikrovlákne chráňte funkciu pred povlakom a predávkovaním.",
                        "Ak textil stráca savosť, pružnosť alebo odvod vlhkosti, nezvyšujte vôňu. Urobte kontrolný cyklus s primeraným gélom a bez zbytočných doplnkov. Trvalé materiálové poškodenie parfum neopraví.",
                    ],
                ),
                (
                    "Diagnostika slabého alebo nepríjemného výsledku",
                    [
                        "Slabá vôňa môže súvisieť s malou dávkou, ale aj s preplnením, tvrdou vodou, nadbytkom gélu, intenzívnym plákaním, materiálom alebo teplom. Nepríjemný výsledok môže byť zmes parfumu a zatuchnutého zdroja.",
                        "Postupujte po jednej premennej: vyčistite práčku, overte gél, náplň a sušenie, potom porovnajte dávku. Pri pachu použite aj návod <a href=\"/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia\">prečo oblečenie zapácha po praní</a>.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Fáza", "Rozhodnutie", "Kontrola"],
                "rows": [
                    ["Výber", "rodina a vzorka", "citlivosť domácnosti"],
                    ["Pranie", "gél a dávka", "štítok a náplň"],
                    ["Oplach", "bez nánosov", "povrch a funkcia"],
                    ["Sušenie", "primerané teplo", "vôňa po vychladnutí"],
                ],
            },
            "steps": [
                "Vyberte vonnú rodinu a malú vzorku.",
                "Skontrolujte materiály, štítky a citlivosť domácnosti.",
                "Pripravte čistú, roztriedenú a nepreplnenú náplň.",
                "Odmerajte detergent a parfum podľa ich etikiet.",
                "Spustite vhodný program a po cykle hneď vyberte bielizeň.",
                "Úplne vysušte a oddelene posúďte čistotu, funkciu a vôňu.",
                "Pri ďalšom teste zmeňte iba jednu premennú.",
            ],
            "checks": [
                ["Kompatibilita", "Povoľuje štítok zvolený cyklus a je výrobok určený do prania?"],
                ["Proces", "Sú dávka, náplň, voda a priehradka nastavené podľa návodu?"],
                ["Výsledok", "Je textil čistý, funkčný, suchý a vôňa príjemná aj po čase?"],
            ],
            "expert": [
                "Prací proces je sústava prepojených premenných. Zmena detergentu mení odstránenie nečistôt a oplach, materiál mení sorpciu a sušenie mení uvoľňovanie prchavých zložiek. Preto úplný sprievodca nemôže zúžiť výsledok na jednu dávku.",
                "Bezpečnostný rámec vonných surovín pracuje s kategóriami použitia a koncentráciou. Spotrebiteľ má rešpektovať etiketu hotového výrobku a citlivosť členov domácnosti; výraznejší senzorický výsledok nie je nadradený tolerancii.",
            ],
            "sources": [["IFRA Code of Practice", IFRA], ["Laundry malodour and textile microbiome", LAUNDRY_MALODOUR], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
            "commerce": FRAGRANCE_COMMERCE,
            "faq": [
                ["Nahrádza parfum do prania prací gél?", "Nie. Na čistotu použite vhodný detergent a parfum pridávajte ako samostatnú vonnú vrstvu."],
                ["Ako zvoliť prvú dávku?", "Začnite spodnou odporúčanou dávkou a výsledok posúďte na suchej bielizni."],
                ["Prečo vôňa nevydrží?", "Skontrolujte materiál, dávku, náplň, plákanie, vodu a sušenie, nie iba samotný parfum."],
                ["Je vhodný na každý textil?", "Nie automaticky. Rozhoduje štítok, funkcia materiálu a etiketa výrobku."],
                ["Ako porovnať dve vône?", "Pri rovnakom textile, dávke, programe a sušení, ideálne pomocou vzoriek."],
            ],
        },
        {
            "post_id": "1921",
            "title": "Ako kombinovať prací gél a parfum do prania",
            "slug": "ako-kombinovat-praci-gel-a-parfum-do-prania",
            "short": "Prací gél a parfum do prania používajte ako dve oddelené funkcie, v určených priehradkách a dávkach; nemiešajte ich v jednej odmerke.",
            "description": "Rýchly rozhodovací návod na kombinovanie pracieho gélu a parfumu do prania: priehradky, dávky, kompatibilita, prvý test a chyby.",
            "quick": "Gél dávkujte na čistenie podľa znečistenia a vody, parfum na vôňu podľa vlastnej etikety. Každý výrobok vložte do určenej priehradky alebo použite určeným spôsobom. Nekombinujte ich v jednej nádobe naslepo a pri prvom teste použite menšiu dávku parfumu.",
            "intro": "Tento článok je stručný kompatibilitný a dávkovací rozhodovací strom. Podrobný článok s podobnou témou rieši problémy podľa materiálu, tvrdej vody, nánosov a konkrétneho neželaného výsledku.",
            "focus": "základné rozdelenie funkcií, priehradky, poradie, prvý test, dávku a okamžité rozhodnutie pri chybe",
            "boundary": "hĺbkovú diagnostiku zápachu, povlaku, tvrdosti vody, savosti a športových materiálov po nevydarenom cykle",
            "points": [
                "gél a parfum majú oddelenú funkciu aj samostatné dávkovanie",
                "etiketa konkrétneho výrobku má prednosť pred všeobecným internetovým návodom",
                "miešanie koncentrátov v odmerke môže zmeniť rozptýlenie a vytvoriť lokálny kontakt",
                "pri prvom teste meníme iba parfum, nie celý prací proces",
            ],
            "sections": [
                (
                    "Ktorý výrobok robí čo",
                    [
                        "Prací gél obsahuje zložky na odstránenie mastnoty, častíc a ďalšieho znečistenia. Parfum do prania je vonná vrstva. Slabé pranie sa nerieši vyššou dávkou parfumu a slabá vôňa automaticky neznamená pridať viac gélu.",
                        "Ak výrobok deklaruje aj inú funkciu, overte ju na etikete. Názov vône alebo olejový vzhľad nie je technický návod. Nepoužívajte osobný parfum ani interiérový sprej ako náhradu.",
                    ],
                ),
                (
                    "Priehradky a poradie",
                    [
                        "Gél patrí do priehradky alebo bubna podľa svojho návodu. Parfum použite presne tam, kde uvádza jeho výrobca. Symbol kvetu na práčke môže označovať avivážnu priehradku, no overte manuál spotrebiča a etiketu.",
                        "Koncentráty vopred nemiešajte v uzavretej odmerke. Môžu mať odlišné nosiče a fázu vyplavenia. Pri náhodnom naliatí na textil cyklus nespúšťajte naslepo; miesto opláchnite podľa pokynov výrobcu a materiálu.",
                    ],
                ),
                (
                    "Ako nastaviť dve dávky",
                    [
                        "Gél vypočítajte podľa náplne, znečistenia a tvrdosti vody. Parfum začnite spodnou odporúčanou dávkou. Zníženie jedného výrobku sa nekompenzuje automatickým zvýšením druhého, pretože ich úlohy nie sú rovnaké.",
                        "Pri menšej náplni upravte dávky podľa etikiet. Polovičný bubon nemusí vždy znamenať presne polovicu každého výrobku, ak návod používa iné pásma; nevytvárajte vlastnú lineárnu schému bez údajov.",
                    ],
                ),
                (
                    "Rýchle rozhodnutie po cykle",
                    [
                        "Ak je bielizeň čistá a vôňa slabá, ďalší test môže zmeniť iba parfum. Ak je špinavá alebo zapácha, najprv riešte gél, program a sušenie. Ak je klzká alebo mapovitá, overte predávkovanie a oplach.",
                        "Detailnú diagnostiku nájdete v článku <a href=\"/n/praci-gel-a-parfum-do-prania-ako-ich-kombinovat-aby-pradlo-cistilo-aj-vonalo\">prací gél a parfum do prania podľa materiálu a problému</a>.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Výsledok", "Najprv riešte", "Nemeňte naslepo"],
                "rows": [
                    ["Čisté, slabo voňavé", "dávku parfumu", "gél"],
                    ["Zapáchajúce", "pranie a sušenie", "iba vôňu"],
                    ["Klzké alebo tuhé", "dávku a oplach", "ďalší produkt"],
                    ["Lokálna mapa", "priamy kontakt", "teplo a sušičku"],
                ],
            },
            "steps": [
                "Prečítajte etikety gélu, parfumu a návod práčky.",
                "Roztrieďte náplň a skontrolujte ošetrovacie štítky.",
                "Odmerajte gél podľa znečistenia a vody.",
                "Odmerajte parfum samostatne podľa jeho etikety.",
                "Vložte každý výrobok do určeného miesta bez miešania.",
                "Po cykle náplň úplne vysušte.",
                "Podľa výsledku zmeňte nabudúce iba jednu dávku alebo podmienku.",
            ],
            "checks": [
                ["Funkcia", "Viete, ktorý výrobok rieši čistotu a ktorý iba vôňu?"],
                ["Umiestnenie", "Potvrdzujú etikety a manuál správnu priehradku pre oba výrobky?"],
                ["Výsledok", "Je problém v čistote, vôni, oplachu alebo sušení?"],
            ],
            "expert": [
                "Detergentná a parfumová formulácia sa optimalizujú pre odlišné úlohy. Ich priame predmiešanie môže zmeniť lokálnu koncentráciu a čas kontaktu, preto má prednosť dávkovací systém a návod hotového výrobku.",
                "Pri diagnostike sa oddeľuje prací výkon, zvyškový povlak a senzorická intenzita. Jedno čuchové hodnotenie bez kontroly čistoty a suchosti nedokáže určiť, ktorý výrobok alebo krok treba upraviť.",
            ],
            "sources": [["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE], ["IFRA Code of Practice", IFRA]],
            "commerce": FRAGRANCE_COMMERCE,
            "faq": [
                ["Môžem zmiešať gél a parfum v jednej odmerke?", "Nie bez výslovného pokynu výrobcu. Použite ich oddelene určeným spôsobom."],
                ["Ktorý výrobok ide do ktorej priehradky?", "Overte etikety oboch výrobkov a manuál konkrétnej práčky."],
                ["Musím znížiť gél, keď pridám parfum?", "Nie automaticky. Majú odlišné funkcie a každý sa dávkuje podľa vlastného návodu."],
                ["Čo zvýšiť pri slabej vôni?", "Až po kontrole čistoty a sušenia urobte porovnateľný test s malou zmenou dávky parfumu."],
                ["Prečo je bielizeň klzká?", "Častou príčinou je nadbytok výrobku, slabý oplach alebo preplnený bubon."],
            ],
        },
        {
            "post_id": "1868",
            "title": "Prací gél a parfum do prania: ako ich kombinovať, aby prádlo čistilo aj voňalo",
            "slug": "praci-gel-a-parfum-do-prania-ako-ich-kombinovat-aby-pradlo-cistilo-aj-vonalo",
            "short": "Keď kombinácia gélu a parfumu nefunguje, diagnostika sa riadi materiálom, tvrdosťou vody, znečistením, nánosmi, oplachom a sušením.",
            "description": "Hĺbková diagnostika gélu a parfumu do prania: tvrdá voda, syntetika, uteráky, športové oblečenie, zvyšky, pach a slabá vôňa.",
            "quick": "Ak bielizeň po kombinácii gélu a parfumu nie je čistá, najprv opravte prací proces. Ak je čistá, ale nevonia, skontrolujte materiál, dávku, priehradku, vodu a sušenie. Ak je klzká, tvrdá alebo mapovitá, znížte nánosy a zlepšite oplach skôr, než pridáte ďalší výrobok.",
            "intro": "Tento článok je hĺbkový diagnostický sprievodca pre nevydarený výsledok. Rýchly článok s podobným názvom rieši iba základné priehradky, oddelené dávky a prvý test.",
            "focus": "problémy podľa bavlny, syntetiky, uterákov a športového textilu, tvrdú vodu, nánosy, pach, slabú vôňu a systematickú opravu",
            "boundary": "základné rozdelenie funkcií a okamžitú odpoveď, kam výrobky vložiť pri bezproblémovom prvom cykle",
            "points": [
                "diagnostika začína oddelením čistoty, funkcie textilu a vône",
                "tvrdá voda a dávka gélu môžu nepriamo meniť výsledok parfumu",
                "uterák, syntetika a membrána reagujú na nánosy rozdielne",
                "pri oprave meníme jednu premennú a výsledok hodnotíme na suchom textile",
            ],
            "sections": [
                (
                    "Tvrdá voda a zvyšky výrobkov",
                    [
                        "Tvrdšia voda zvyšuje nároky na detergent a môže prispieť k minerálnemu povlaku. Príliš vysoká dávka však vytvorí ďalší zvyšok. Použite pásmo tvrdosti z etikety a porovnajte kontrolný cyklus s primeranou náplňou.",
                        "Biela mapa alebo tuhý povrch nie sú dôvodom pridať viac parfumu. Urobte oplach, vyčistite zásobník a skontrolujte, či sa výrobky vyplavujú v správnej fáze. Až potom dolaďujte vôňu.",
                    ],
                ),
                (
                    "Uteráky a savosť",
                    [
                        "Uterák môže pôsobiť mäkko a voňať, no menej sať, ak sa na slučkách hromadí povlak. Pri skúške vynechajte aviváž, použite primeraný gél a sledujte, či sa voda po povrchu rozlieva alebo rýchlo vsiakne.",
                        "Ak savosť rastie po niekoľkých cykloch s lepším oplachom, problém bol pravdepodobne v nánosoch. Ďalšie postupy ponúka článok <a href=\"/n/ako-zmaekcit-uteraky\">ako zmäkčiť uteráky</a>.",
                    ],
                ),
                (
                    "Syntetika a športové oblečenie",
                    [
                        "Syntetické vlákna zadržiavajú kožný maz a pach inak než bavlna. Silnejšia vôňa môže po zahriatí pri nosení vytvoriť nepríjemnú zmes. Potrebný je vhodný gél, dostatok času a rýchle sušenie.",
                        "Pri mikrovlákne, membráne a funkčných úpravách chráňte povrch pred nevhodným zmäkčovadlom a predávkovaním. Štítok a výrobca odevu majú prednosť pred univerzálnym odporúčaním.",
                    ],
                ),
                (
                    "Slabá vôňa verzus neželaný pach",
                    [
                        "Slabá čistá vôňa je otázka dávky a procesu. Kyslý, zatuchnutý alebo mastný pach ukazuje na zvyškové znečistenie, práčku alebo sušenie. Tieto dve situácie sa nesmú riešiť rovnakým navýšením parfumu.",
                        "Najprv vyperte kontrolnú náplň v čistej práčke, s vhodným gélom a úplným sušením. Ak je čistá a bez pachu, skúšajte parfum po malých krokoch. Pri pretrvávaní pachu riešte zdroj.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Materiál alebo prejav", "Pravdepodobný problém", "Úprava"],
                "rows": [
                    ["Uterák nesaje", "nános", "oplach a menej zmäkčovadla"],
                    ["Syntetika zapácha", "maz a pomalé sušenie", "vhodný cyklus a vzduch"],
                    ["Biele mapy", "gél alebo minerály", "dávka a oplach"],
                    ["Čisté, slabo voňavé", "nízka senzorická intenzita", "malý test dávky parfumu"],
                ],
            },
            "steps": [
                "Pomenujte problém ako čistotu, funkciu, povlak, pach alebo vôňu.",
                "Skontrolujte materiál, štítok, vodu a stav práčky.",
                "Nastavte vhodný gél a primeranú nepreplnenú náplň.",
                "Pri nánosoch urobte kontrolný oplach bez ďalších doplnkov.",
                "Parfum ponechajte na stabilnej malej dávke počas diagnostiky.",
                "Náplň úplne vysušte a nechajte vychladnúť.",
                "Zmeňte jednu premennú a výsledok znovu porovnajte.",
            ],
            "checks": [
                ["Kategória problému", "Je textil špinavý, poškodený, povlečený alebo iba slabo voňavý?"],
                ["Materiál", "Ide o savú bavlnu, syntetiku, mikrovlákno alebo funkčnú úpravu?"],
                ["Proces", "Sú voda, dávka, náplň, oplach a sušenie stabilné?"],
            ],
            "expert": [
                "Adsorpcia nečistôt a vonných látok závisí od polarity a štruktúry vlákna. Polyester a bavlna preto nereagujú rovnako na kožný maz, detergent ani sušenie. Materiálová diagnostika je presnejšia než univerzálne zvýšenie dávky.",
                "Výskum pachu bielizne spája výsledok s mikrobiálnym a chemickým profilom textilu. Vôňa môže senzorický vnem meniť, ale pre stabilný výsledok treba odstrániť zdroj a skrátiť čas vo vlhku.",
            ],
            "sources": [["Laundry malodour and textile microbiome", LAUNDRY_MALODOUR], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
            "commerce": FRAGRANCE_COMMERCE,
            "faq": [
                ["Prečo uterák po géle a parfume menej saje?", "Často pre nános výrobkov alebo slabý oplach; otestujte cyklus bez aviváže a s primeranou dávkou."],
                ["Prečo športové oblečenie po zahriatí zapácha?", "Vo vláknach mohli zostať maz a organické zvyšky, ktoré sa pri teple znovu prejavia."],
                ["Pomôže viac parfumu pri tvrdej vode?", "Nie automaticky. Najprv upravte dávku detergentu a skontrolujte minerálne či pracie zvyšky."],
                ["Čo znamená klzká bielizeň?", "Môže signalizovať nadbytok výrobku alebo nedostatočné plákanie."],
                ["Kedy meniť parfum?", "Až keď je prací proces stabilný, textil čistý a problém zostáva iba v senzorickej preferencii."],
            ],
        },
    ]
)

CONFIGS.extend(
    [
        {
            "post_id": "1967",
            "title": "Ako prať športové uteráky, aby nezapáchali",
            "slug": "ako-prat-sportove-uteraky-aby-nezapachali",
            "short": "Zápachu športových uterákov predchádza najmä rýchle vysušenie po použití, oddelený kôš, primeraná dávka gélu a úplné dosušenie po praní.",
            "description": "Ako zabrániť zápachu športových uterákov: režim po tréningu, kôš na bielizeň, pranie, oplach, sušenie a riešenie zatuchnutia.",
            "quick": "Mokrý športový uterák nenechávajte zrolovaný v taške. Po tréningu ho rozveste, perte ho v primeranom čase podľa štítku, nepredávkujte gél ani aviváž a po cykle ho úplne vysušte. Ak pach zostáva, riešte nánosy, práčku a čas vo vlhku, nie iba silnejšiu vôňu.",
            "intro": "Tento článok je preventívny návod proti pachu od chvíle, keď uterák opustí fitko. Samostatný sprievodca športovými uterákmi rieši výber programu podľa bavlny, mikrovlákna a prevádzkového používania.",
            "focus": "čas vo vlhku, tašku a kôš, vznik pachu, primeranú dávku, obmedzenie nánosov a rýchle úplné sušenie",
            "boundary": "porovnanie bavlneného a mikrovláknového uteráka, pranie viacerých uterákov z fitka a prevádzková hygiena",
            "points": [
                "najväčšie riziko vzniká počas hodín, keď je uterák mokrý a zrolovaný",
                "parfum môže doplniť čistotu, ale neodstráni mikrobiálny alebo zatuchnutý zdroj pachu",
                "priveľa gélu a aviváže môže zhoršiť oplach aj savosť",
                "uterák musí po praní vyschnúť v celej hrúbke vrátane lemov",
            ],
            "sections": [
                (
                    "Čo sa deje v taške po tréningu",
                    [
                        "Vlhkosť, teplo, kožný maz a organické zvyšky vytvárajú prostredie, v ktorom sa pach rýchlo rozvíja. Zrolovaný uterák má malú plochu na odparovanie a stred zostáva vlhký oveľa dlhšie než povrch.",
                        "Ak nemôžete prať hneď, uterák aspoň rozložte na vzdušné miesto. Nevkladajte ho mokrý medzi suché oblečenie a nezatvárajte do nepriedušného koša. Po návrate domov vyprázdnite športovú tašku.",
                    ],
                ),
                (
                    "Ako nastaviť pranie proti pachu",
                    [
                        "Zvoľte najvyššiu teplotu povolenú štítkom a program, ktorý uterák dostatočne pohybuje a opláchne. Bavlna často znesie viac než tenké mikrovlákno, preto ich nemiešajte automaticky s rovnakým režimom.",
                        "Gél dávkujte podľa náplne, znečistenia a tvrdosti vody. Nadbytok môže zostať v slučkách a zachytávať ďalšie nečistoty. Ak je uterák klzký alebo menej savý, urobte kontrolné pranie bez aviváže.",
                    ],
                ),
                (
                    "Kedy zápach ukazuje na práčku",
                    [
                        "Ak podobne zapácha aj bežná bielizeň, skontrolujte tesnenie, zásobník, filter a odtok. Pach sa môže vracať z biofilmu alebo stojatej vody v spotrebiči, najmä pri dlhodobom používaní iba krátkych nízkoteplotných cyklov.",
                        "Porovnajte jeden cyklus v čistej práčke s rovnakou dávkou a náplňou. Ak sa výsledok zlepší, problém nebol iba v uteráku. Súvisiace príčiny rozoberá článok <a href=\"/n/preco-moje-oblecenie-zapacha-po-prani-priciny-a-riesenia\">prečo oblečenie zapácha po praní</a>.",
                    ],
                ),
                (
                    "Sušenie a skladovanie",
                    [
                        "Po cykle uterák pretrepte, rozložte a zabezpečte prúdenie vzduchu z oboch strán. Hrubý lem môže zostať vlhký, aj keď je stred na dotyk suchý. Pri sušičke rešpektujte štítok a nepresušujte.",
                        "Do skrine patrí iba úplne suchý uterák. Ak je pach cítiť po navlhčení, hoci suchý kus pôsobí čisto, skúste dôkladnejší cyklus, lepší oplach a odstránenie nánosov. Silnejšia vôňa by problém iba prekryla.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Moment", "Riziko", "Prevencia"],
                "rows": [
                    ["Po tréningu", "vlhká rolka", "hneď rozvesiť"],
                    ["V koši", "uzavretá vlhkosť", "vzdušné oddelenie"],
                    ["Pri praní", "nános výrobku", "primeraná dávka a oplach"],
                    ["Po praní", "vlhký lem", "úplne dosušiť"],
                ],
            },
            "steps": [
                "Po použití uterák rozložte a nechajte odpariť vlhkosť.",
                "Pred praním ho oddeľte od jemných a veľmi špinavých kusov.",
                "Skontrolujte materiál a maximálnu teplotu na štítku.",
                "Použite primeranú dávku gélu bez automatickej aviváže.",
                "Zvoľte program s dostatočným časom a oplachom.",
                "Po cykle uterák okamžite vyberte a rozložte.",
                "Savosť a pach overte až po úplnom vysušení a opätovnom navlhčení.",
            ],
            "checks": [
                ["Čas vo vlhku", "Koľko hodín zostal uterák po tréningu zrolovaný?"],
                ["Nánosy", "Je povrch klzký, tuhý alebo menej savý než predtým?"],
                ["Práčka", "Zapácha rovnakým spôsobom aj iná čerstvo vypraná bielizeň?"],
            ],
            "expert": [
                "Novšie práce o mikrobiome pranej bielizne skúmajú, ako zloženie textilu, kožné mikroorganizmy, prací proces a sušenie súvisia s opakovaným vznikom pachu. Praktickým dôsledkom je potreba odstrániť organické zvyšky a skrátiť čas vo vlhku.",
                "Hygienický výkon domáceho prania nevytvára iba teplota. Dôležitý je detergent, mechanika, čas, oplach a úplné sušenie; pri uteráku navyše rozhoduje hrúbka a savosť konštrukcie.",
            ],
            "sources": [["Laundry malodour and textile microbiome", LAUNDRY_MALODOUR], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Môžem nechať mokrý uterák v športovej taške do rána?", "Nie je to vhodné. Rozviňte ho čo najskôr, aby sa skrátil čas vo vlhku."],
                ["Pomôže viac gélu proti pachu?", "Nie automaticky. Nadbytok sa môže horšie vypláchnuť a vytvoriť nános."],
                ["Mám použiť aviváž?", "Pri uterákoch môže nadmerné používanie znížiť savosť; riaďte sa štítkom a funkciou textilu."],
                ["Prečo uterák zapácha až po navlhčení?", "Vo vláknach mohli zostať organické zvyšky alebo nánosy, ktoré sa pri vlhkosti znovu prejavia."],
                ["Je potrebné uterák vyvárať?", "Nie. Použite iba teplotu povolenú štítkom a riešte celý proces vrátane sušenia."],
            ],
        },
        {
            "post_id": "2069",
            "title": "Ako prať športové uteráky a uteráky do fitka",
            "slug": "ako-prat-sportove-uteraky-a-uteraky-do-fitka",
            "short": "Bavlnený a mikrovláknový uterák do fitka sa nemusia prať rovnako; program vyberajte podľa materiálu, savosti, frekvencie a štítku.",
            "description": "Pranie uterákov do fitka podľa materiálu: bavlna, mikrovlákno, malé rýchloschnúce uteráky, dávka gélu, hygiena a sušenie.",
            "quick": "Uteráky do fitka roztrieďte podľa materiálu a farby, perte ich v programe povolenom štítkom, nepoužívajte priveľa gélu ani zmäkčovadla a úplne ich vysušte. Mikrovlákno potrebuje šetrnejšiu teplotu a čistý povrch vlákien; hrubá bavlna zas dostatočný oplach a dlhšie sušenie.",
            "intro": "Tento článok je materiálový a prevádzkový návod: porovnáva druhy športových uterákov, frekvenciu prania a vhodnú náplň. Prevencii zatuchnutia počas pobytu v taške sa venuje samostatný článok.",
            "focus": "rozdiel bavlny a mikrovlákna, frekvenciu, spoločné pranie, mechaniku, savosť, oplach a prevádzkové používanie vo fitku",
            "boundary": "príčiny pachu počas hodín vo vlhkej taške a diagnostiku zapáchajúcej práčky",
            "points": [
                "materiál uteráka určuje teplotu, mechaniku aj spôsob sušenia",
                "mikrovlákno neperte s textíliami, ktoré uvoľňujú veľa vlákien a chlpov",
                "spoločný uterák alebo opakované použitie zvyšuje hygienické nároky",
                "savosť sa hodnotí po odstránení nánosov a úplnom vysušení",
            ],
            "sections": [
                (
                    "Bavlna verzus mikrovlákno",
                    [
                        "Bavlnený uterák používa slučky a hydrofilný povrch, býva odolnejší, ale schne pomalšie. Mikrovlákno je ľahké a rýchloschnúce, no jeho jemná štruktúra zachytáva mastnotu a môže stratiť funkciu pri povlaku zo zmäkčovadla.",
                        "Riaďte sa štítkom konkrétneho výrobku. Mikrovlákno často potrebuje nižšiu teplotu a pranie bez aviváže, bavlna môže tolerovať vyššiu teplotu. Neprenášajte pravidlo z jedného uteráka na všetky ostatné.",
                    ],
                ),
                (
                    "Ako často uterák z fitka prať",
                    [
                        "Uterák nasiaknutý potom, položený na strojoch alebo použitý v spoločnej sprche perte po každom použití. Malý uterák, ktorý zostal suchý a slúžil iba na ruky, môže mať inú situáciu, no vždy zohľadnite osobnú hygienu a podmienky fitka.",
                        "Nezdieľajte uterák medzi ľuďmi. Ak máte kožný problém alebo odporúčanie zdravotníka, riaďte sa ním. Čistý náhradný uterák v samostatnom vrecku je praktickejší než opakované používanie vlhkého kusa.",
                    ],
                ),
                (
                    "S čím uteráky prať",
                    [
                        "Hrubé bavlnené uteráky možno spojiť s podobnou farebnou a materiálovou náplňou. Mikrovlákno oddeľte od froté, flísu a textílií púšťajúcich chĺpky, ktoré sa zachytia v jemnej štruktúre a znížia čistiacu či savú funkciu.",
                        "Nevkladajte uteráky k jemnej športovej bielizni so zipsami, háčikmi a elastanom bez rozmyslu. Hmotnosť mokrej bavlny a mechanické trenie môžu jemné vrstvy zbytočne zaťažiť.",
                    ],
                ),
                (
                    "Dávka, plákanie a kontrola savosti",
                    [
                        "Prací gél odmerajte podľa návodu. Pri malých športových uterákoch sa ľahko použije dávka pre plný bubon, ktorá sa potom zle vyplachuje. Doplňujúci oplach má zmysel pri viditeľnom povlaku, nie ako automatická náhrada správnej dávky.",
                        "Ak uterák nesaje, skúste niekoľko cyklov bez aviváže a s primeraným gélom. Ďalšie možnosti nájdete v článku <a href=\"/n/ako-zmaekcit-uteraky\">ako zmäkčiť uteráky</a>. Trvalé poškodenie vlákna však pranie nevráti.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Typ", "Silná stránka", "Starostlivosť"],
                "rows": [
                    ["Bavlnené froté", "vysoká savosť", "dôkladný oplach a sušenie"],
                    ["Mikrovlákno", "rýchle schnutie", "bez aviváže a chĺpkov"],
                    ["Zmesový uterák", "nižšia hmotnosť", "podľa štítku zmesi"],
                    ["Spoločný uterák", "žiadna výhoda", "nezdieľať a prať po použití"],
                ],
            },
            "steps": [
                "Po použití oddeľte uterák od čistých vecí a nechajte ho dýchať.",
                "Pred praním identifikujte materiál, farbu a povolenú teplotu.",
                "Bavlnu a mikrovlákno rozdeľte, ak potrebujú iný program.",
                "Naplňte bubon podobnými textíliami bez preplnenia.",
                "Odmerajte gél a vynechajte nevhodné zmäkčovadlo.",
                "Po cykle uteráky pretrepte a úplne vysušte.",
                "Skontrolujte savosť, pach a zachytené chĺpky pred ďalším použitím.",
            ],
            "checks": [
                ["Materiál", "Je uterák bavlnený, mikrovláknový alebo zmesový?"],
                ["Použitie", "Bol v kontakte s potom, spoločnými povrchmi alebo sprchou?"],
                ["Funkcia", "Saje po vysušení bez klzkého povlaku a zachytených chĺpkov?"],
            ],
            "expert": [
                "Textilná konštrukcia mení zadržiavanie vody, organických zvyškov aj čas sušenia. Preto materiálovo odlišné uteráky nemajú automaticky rovnakú optimálnu teplotu a mechaniku, hoci slúžia v rovnakom fitku.",
                "Odborný prehľad hygieny domáceho prania zdôrazňuje kombináciu prania a sušenia. Pri uterákoch používaných na spoločných miestach je dôležité aj individuálne používanie a čisté skladovanie pred tréningom.",
            ],
            "sources": [["GINETEX: symboly ošetrovania", GINETEX], ["Hygiene of domestic laundry: review", LAUNDRY_HYGIENE]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Môžem prať mikrovlákno s bavlneným froté?", "Radšej ich oddeľte, ak froté púšťa chĺpky alebo štítky vyžadujú odlišný program."],
                ["Ako často prať uterák do fitka?", "Po každom použití, ak nasiakol potom alebo bol v kontakte so spoločnými povrchmi."],
                ["Je aviváž vhodná na mikrovlákno?", "Často nie, pretože povlak môže znížiť funkciu jemných vlákien; riaďte sa štítkom."],
                ["Môžem uterák zdieľať?", "Nie je to vhodné. Používajte vlastný čistý uterák."],
                ["Prečo uterák menej saje?", "Častou príčinou sú nánosy výrobkov, zachytené chĺpky, poškodenie alebo presušenie."],
            ],
        },
        {
            "post_id": "1990",
            "title": "Ako prať kúpeľňové predložky, aby nezapáchali",
            "slug": "ako-prat-kupelnove-predlozky-aby-nezapachali",
            "short": "Zápachu kúpeľňovej predložky predchádza sušenie po každom sprchovaní, čistá podlaha pod ňou a pranie skôr, než sa vlhkosť uzavrie v podklade.",
            "description": "Ako zabrániť zápachu kúpeľňovej predložky: vetranie, sušenie, podlaha pod rohožou, frekvencia prania a bezpečná kontrola plesne.",
            "quick": "Predložku po sprchovaní zdvihnite z mokrej podlahy, nechajte ju vyschnúť z oboch strán a kúpeľňu vetrajte. Perte ju podľa štítku skôr, než sa v podklade vytvorí trvalý pach. Ak vidíte pleseň, drobenie gumy alebo lepkavý podklad, najprv posúďte stav výrobku, nie silu parfumu.",
            "intro": "Tento článok rieši prevenciu zápachu medzi praniami a vlhkostný režim kúpeľne. Samostatný návod na kúpeľňové rohože rozoberá mechanické pranie podľa gumy, latexu, peny a textilného podkladu.",
            "focus": "vlhkosť po sprche, sušenie oboch strán, podlahu pod predložkou, frekvenciu prania, vetranie a rozpoznanie poškodenia",
            "boundary": "voľbu programu, otáčok a vyváženie práčky pri rôznych typoch protišmykového podkladu",
            "points": [
                "pach vzniká často medzi praniami, keď predložka leží mokrá na podlahe",
                "sušiť treba textilnú vrstvu aj spodný protišmykový podklad",
                "vôňa nesmie zakryť pleseň, degradáciu gumy alebo špinavú podlahu",
                "frekvenciu určujú používanie, schnutie a stav, nie pevný kalendár",
            ],
            "sections": [
                (
                    "Prečo predložka zapácha aj po praní",
                    [
                        "Hrubá vrstva a nepriepustný podklad schnú pomalšie než uterák. Ak predložka leží na mokrej dlažbe, voda zostáva medzi povrchmi a pach sa môže vracať aj po krátkom cykle.",
                        "Zdrojom môže byť aj podlaha, škára alebo nedostatočne vetraná kúpeľňa. Pri diagnostike vyčistite a vysušte obe plochy oddelene. Parfumovaný výrobok nedokáže odstrániť vlhkostnú príčinu.",
                    ],
                ),
                (
                    "Rutina po každom sprchovaní",
                    [
                        "Predložku zaveste alebo preložte tak, aby vzduch prúdil k lícu aj spodnej strane. Ventilátor alebo otvorené okno používajte podľa možností kúpeľne. Mokré uteráky neklaďte na predložku.",
                        "Podlahu pod ňou utrite, ak zostáva voda. Raz za čas skontrolujte rohy, lemy a spodok. Tmavé bodky, slizký film, drobenie alebo trvalý zatuchnutý pach vyžadujú podrobnejšie riešenie.",
                    ],
                ),
                (
                    "Kedy predložku prať",
                    [
                        "Častejšie pranie potrebuje domácnosť s viacerými ľuďmi, slabým vetraním, domácim zvieraťom alebo predložkou, ktorá je denne úplne mokrá. Dobre schnúci kus v suchej kúpeľni má inú záťaž.",
                        "Nečakajte iba na viditeľnú špinu. Signálom je pomalšie schnutie, pach po navlhčení, mastný povrch alebo nečistota pod predložkou. Program a teplotu však vždy určuje štítok a stav podkladu.",
                    ],
                ),
                (
                    "Pleseň alebo poškodený podklad",
                    [
                        "Pri povrchovej nečistote postupujte podľa etikety vhodného čističa a materiálu. Nikdy nemiešajte chlórové bielidlo s octom, kyselinami alebo inými čističmi. Kúpeľňu počas práce vetrajte.",
                        "Ak sa podklad odlupuje, lepí, praská alebo zostáva trvalo zapáchajúci, ďalšie pranie môže uvoľniť kúsky do filtra a zhoršiť protišmykovú funkciu. Vtedy je bezpečnejšia výmena než agresívnejší cyklus.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Signál", "Možná príčina", "Prvý krok"],
                "rows": [
                    ["Pach po sprche", "pomalé schnutie", "zavesiť z oboch strán"],
                    ["Mokrá dlažba pod kusom", "uzavretá voda", "utrieť podlahu"],
                    ["Tmavé bodky", "možná pleseň", "oddeliť a posúdiť materiál"],
                    ["Drobenie podkladu", "degradácia", "neprať agresívnejšie"],
                ],
            },
            "steps": [
                "Po sprche predložku zdvihnite z mokrej podlahy.",
                "Zabezpečte prúdenie vzduchu k lícu aj podkladu.",
                "Pravidelne utrite a vysušte podlahu pod ňou.",
                "Pred praním skontrolujte pach, body, lemy a drobenie.",
                "Perte iba programom povoleným štítkom.",
                "Po cykle ju vytvarujte a úplne vysušte bez ostrého tepla.",
                "Do kúpeľne ju vráťte až po vysušení oboch strán.",
            ],
            "checks": [
                ["Spodná strana", "Je podklad suchý, celistvý a bez tmavých bodiek?"],
                ["Podlaha", "Nezostáva pod predložkou voda alebo nečistota?"],
                ["Pach", "Objaví sa až po navlhčení, alebo je trvalý aj v suchom stave?"],
            ],
            "expert": [
                "Biologické znečistenie v interiéri podporuje pretrvávajúca vlhkosť. Základným opatrením je kontrola zdroja vody a sušenie; chemický výrobok bez zmeny vlhkostných podmienok nemá dlhodobý efekt.",
                "EPA pri domácich čističoch zdôrazňuje etiketu, správne riedenie, kompatibilitu povrchu, vetranie a zákaz miešania produktov. Tieto pravidlá platia aj pri čistení podlahy pod predložkou.",
            ],
            "sources": [["US EPA: biological contaminants and safe cleaning", EPA_CLEANING], ["GINETEX: symboly ošetrovania", GINETEX]],
            "commerce": CLEANING_COMMERCE,
            "faq": [
                ["Ako často prať kúpeľňovú predložku?", "Podľa používania a schnutia; pri dennom premáčaní, pachu alebo pomalom schnutí častejšie."],
                ["Prečo zapácha iba keď je mokrá?", "Vlhkosť môže znovu uvoľniť pach zo zvyškov a mikrobiálneho znečistenia vo vrstvách."],
                ["Môžem použiť ocot a chlórové bielidlo?", "Nie. Nikdy ich nemiešajte, pretože môže vzniknúť nebezpečný plyn."],
                ["Pomôže silnejší parfum?", "Nie, ak zostáva vlhkosť, pleseň, špinavá podlaha alebo poškodený podklad."],
                ["Kedy predložku vymeniť?", "Keď sa podklad drobí, lepí, praská, stráca protišmykovú funkciu alebo trvalo zapácha."],
            ],
        },
        {
            "post_id": "2066",
            "title": "Ako prať kúpeľňové predložky a rohože bez zápachu",
            "slug": "ako-prat-kupelnove-predlozky-a-rohoze-bez-zapachu",
            "short": "Predložku perte podľa typu podkladu a hmotnosti za mokra; gumová, latexová, penová a čisto textilná rohož nemajú rovnaký program.",
            "description": "Pranie kúpeľňových predložiek podľa podkladu: guma, latex, pena, bavlna, vyváženie bubna, otáčky, sušenie a ochrana práčky.",
            "quick": "Najprv identifikujte spodnú vrstvu a prečítajte štítok. Čisto textilná predložka môže zniesť bežnejší cyklus, kým guma, latex, pamäťová pena alebo lepený podklad často vyžadujú nižšiu teplotu, slabšie odstreďovanie alebo ručné čistenie. Poškodený podklad do práčky nedávajte.",
            "intro": "Tento článok rieši techniku samotného prania a ochranu spotrebiča. Prevencia zápachu medzi sprchovaniami, vetranie a kontrola podlahy sú rozpracované v samostatnom návode.",
            "focus": "identifikáciu podkladu, hmotnosť za mokra, vyváženie bubna, teplotu, otáčky, ochranu filtra a sušenie bez deformácie",
            "boundary": "dennú rutinu sušenia v kúpeľni a diagnostiku plesne na podlahe pod predložkou",
            "points": [
                "štítok a stav protišmykového podkladu rozhodujú, či rohož patrí do práčky",
                "mokrá predložka môže byť výrazne ťažšia a rozkolísať nevyvážený bubon",
                "vysoká teplota a prudké odstreďovanie môžu poškodiť lepenie, latex alebo penu",
                "pred praním odstráňte vlasy a drobné časti, ktoré by zaťažili filter",
            ],
            "sections": [
                (
                    "Ako rozpoznať typ spodnej vrstvy",
                    [
                        "Čisto textilná predložka má rovnakú tkaninu alebo slučky z oboch strán. Gumový a latexový podklad je súvislý protišmykový film, penová predložka je hrubšia a stlačiteľná, lepený podklad môže mať viditeľné vrstvy.",
                        "Ak sa spodok lepí, praská alebo púšťa kúsky, pranie v bubne nie je vhodný test. Uvoľnený materiál môže zaniesť filter a predložka stratí bezpečnú priľnavosť k podlahe.",
                    ],
                ),
                (
                    "Príprava pred vložením do práčky",
                    [
                        "Predložku vytraste, povysávajte a odstráňte vlasy. Skontrolujte rohy, lemy a dekorácie. Lokálnu škvrnu ošetrite prostriedkom kompatibilným s materiálom a nenechajte čistič zaschnúť, ak to etiketa nepovoľuje.",
                        "Zvážte veľkosť a nasiakavosť. Veľký kus môže po namočení prekročiť bezpečnú kapacitu práčky. Nesnažte sa ho vyvážiť pridaním množstva jemnej bielizne; použite iba podobné odolné kusy, ak to návod spotrebiča povoľuje.",
                    ],
                ),
                (
                    "Program, teplota a odstreďovanie",
                    [
                        "Zvoľte teplotu a mechaniku podľa štítku. Protišmykové podklady často potrebujú chladnejšiu vodu a nižšie otáčky, pretože teplo a ohýbanie môžu urýchliť degradáciu. Pamäťová pena nemusí byť určená na strojové pranie vôbec.",
                        "Pri prudkom búchaní alebo neschopnosti rozložiť náplň cyklus zastavte podľa návodu práčky. Opakované nevyvážené odstreďovanie zaťažuje spotrebič a nemusí predložku dobre opláchnuť.",
                    ],
                ),
                (
                    "Sušenie bez poškodenia podkladu",
                    [
                        "Predložku vytvarujte a sušte spôsobom povoleným štítkom. Vysoké teplo sušičky alebo radiátora môže poškodiť gumu, latex a lepidlo. Hrubú penu sušte s prúdením vzduchu z viacerých strán.",
                        "Pred položením na dlažbu overte suchý stred aj spodok. Zvyšková vlhkosť uzavretá pri podlahe obnoví pach a môže znížiť protišmykový kontakt. Preventívny režim opisuje článok <a href=\"/n/ako-prat-kupelnove-predlozky-aby-nezapachali\">ako predísť zápachu predložky</a>.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Podklad", "Práčka", "Hlavné riziko"],
                "rows": [
                    ["Čisto textilný", "často podľa štítku", "hmotnosť za mokra"],
                    ["Guma alebo latex", "iba ak povolené", "praskanie a odlepovanie"],
                    ["Pamäťová pena", "často nie", "deformácia a pomalé schnutie"],
                    ["Poškodený lepený", "nevhodné", "kúsky vo filtri"],
                ],
            },
            "steps": [
                "Identifikujte materiál podkladu a prečítajte štítok.",
                "Skontrolujte praskliny, lepenie, drobenie a protišmykovú funkciu.",
                "Vytraste nečistoty a odstráňte vlasy pred praním.",
                "Overte veľkosť a hmotnosť vzhľadom na kapacitu práčky.",
                "Zvoľte povolenú teplotu a primerané otáčky.",
                "Pri nevyvážení postupujte podľa návodu spotrebiča.",
                "Predložku úplne vysušte z oboch strán bez zakázaného tepla.",
            ],
            "checks": [
                ["Podklad", "Je celistvý, pružný a bez lepkavých alebo drobiacich miest?"],
                ["Kapacita", "Môže práčka bezpečne zvládnuť nasiaknutú hmotnosť?"],
                ["Sušenie", "Povoľuje štítok sušičku, alebo iba voľné sušenie?"],
            ],
            "expert": [
                "Nepriepustná spodná vrstva mení nasiakavosť, rozloženie hmotnosti aj odvod vody pri odstreďovaní. Preto sa predložka nedá posudzovať iba podľa textilného povrchu a jej rozmery za sucha nehovoria celý príbeh.",
                "Symboly ošetrovania určujú maximálnu teplotu, miernosť cyklu a možnosti sušenia. Návod práčky je druhým rozhodujúcim zdrojom, pretože stanovuje kapacitu a postup pri nevyváženej náplni.",
            ],
            "sources": [["GINETEX: pranie a sušenie", GINETEX], ["US EPA: bezpečné používanie čistiacich výrobkov", EPA_CLEANING]],
            "commerce": CLEANING_COMMERCE,
            "faq": [
                ["Môžem prať gumovú predložku v práčke?", "Iba ak to povoľuje štítok a podklad nie je poškodený, lepkavý ani popraskaný."],
                ["Prečo práčka pri predložke búcha?", "Nasiaknutý kus môže byť ťažký a nevyvážený; postupujte podľa návodu spotrebiča."],
                ["Môžem ju sušiť na radiátore?", "Nie, ak štítok nepovoľuje teplo; guma, latex a lepidlo sa môžu poškodiť."],
                ["Čo urobiť s drobiacim podkladom?", "Do práčky ho nedávajte. Zvážte výmenu, aby kúsky nezaniesli filter a podklad nestratil funkciu."],
                ["S čím predložku prať?", "Len s podobne odolnými kusmi a iba ak to dovoľuje kapacita a návod práčky."],
            ],
        },
        {
            "post_id": "1881",
            "title": "Ako prať čierne oblečenie, aby nebledlo a nebolo fľakaté",
            "slug": "ako-prat-cierne-oblecenie-aby-nebledlo-a-nebolo-flakate",
            "short": "Keď je čierne oblečenie po praní fľakaté, najprv odlíšte prací povlak, prenos farby, oder a skutočné vyblednutie.",
            "description": "Diagnostika čierneho oblečenia po praní: biele mapy, povlak z gélu, oder, vyblednutie, prenos farby a bezpečný opravný cyklus.",
            "quick": "Bielu mapu na čiernom odeve najprv skúste odstrániť čistým oplachom. Ak mizne, pravdepodobný je zvyšok gélu alebo minerálny povlak; ak zostáva na švoch a miestach trenia, môže ísť o oder alebo stratu pigmentu. Opravný postup voľte až po tejto diagnóze.",
            "intro": "Tento článok je riešenie problému po nevydarenom praní. Preventívny návod na udržanie farby a samostatný výber gélu na čierne oblečenie majú iný cieľ.",
            "focus": "diagnostiku bielych máp, škvŕn a zosivenia po cykle, kontrolný oplach, prenos pigmentu a rozpoznanie nevratného oderu",
            "boundary": "bežnú preventívnu rutinu pred praním a porovnanie vlastností pracích gélov pre tmavú bielizeň",
            "points": [
                "rovnaký svetlý prejav môže byť odstrániteľný povlak alebo trvalá strata farby",
                "opravný cyklus robte bez ďalšej dávky naslepo",
                "miesto škvrny často prezradí viac než jej samotná farba",
                "teplo a sušičku odložte, kým neviete, čo na textile zostalo",
            ],
            "sections": [
                (
                    "Mapa z gélu alebo minerálov",
                    [
                        "Zvyšok sa často drží v záhyboch, vreckách, pri švoch alebo v mieste stlačenej náplne. Môže byť práškový, tuhý alebo klzký. Vzniká pri preplnení, predávkovaní, tvrdej vode alebo slabom oplachu.",
                        "Urobte samostatný oplach bez ďalšieho produktu a po vysušení porovnajte rovnaké miesto. Ak stopa výrazne ustúpila, upravte dávku a náplň. Pri opakovaní skontrolujte zásobník a prietok vody.",
                    ],
                ),
                (
                    "Oder a strata pigmentu",
                    [
                        "Svetlejšie švy, kolená, lakte, golier a hrany vreciek ukazujú skôr na mechanické opotrebovanie. Lícna strana sa môže trieť o bubon, zipsy a hrubšie kusy. Takúto zmenu oplach neodstráni.",
                        "Porovnajte exponované miesto s vnútorným lemom. Ak je vnútro sýte a povrch rovnomerne svetlejší, pigment alebo povrch vlákna sa zmenil. Pranie môže ďalšie opotrebovanie spomaliť, nie vrátiť pôvodnú farbu.",
                    ],
                ),
                (
                    "Prenos cudzej farby alebo škvrna",
                    [
                        "Nepravidelný farebný tón môže pochádzať z nového nestáleho kusa. Mastná mapa, kozmetika alebo nerozpustená kapsula majú iný vzhľad a potrebujú lokálne ošetrenie. Zdroj hľadajte v celej náplni.",
                        "Kým príčinu nepoznáte, nežehlite a nesušte vysokým teplom. Oddelte poškodený kus, otestujte skrytý lem a použite iba postup kompatibilný s materiálom a farbou.",
                    ],
                ),
                (
                    "Ako nastaviť opravný cyklus",
                    [
                        "Pri pravdepodobnom povlaku začnite oplachom alebo miernym praním s minimom vhodného prostriedku. Bubnu nechajte priestor a zvoľte teplotu podľa štítku. Nepridávajte ocot, sódu a ďalší gél naraz.",
                        "Ak výsledok nezmení ani čistý oplach, ďalšie opakovanie môže iba zvýšiť trenie. Vráťte sa k diagnóze oderu, pigmentu alebo materiálového poškodenia a nastavte budúcu prevenciu.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Vzhľad", "Možná príčina", "Test"],
                "rows": [
                    ["Biely záhyb", "zvyšok produktu", "čistý oplach"],
                    ["Svetlé hrany", "oder", "porovnať vnútorný lem"],
                    ["Farebná mapa", "prenos pigmentu", "skontrolovať náplň"],
                    ["Mastný kruh", "lokálna škvrna", "ošetriť bez tepla"],
                ],
            },
            "steps": [
                "Odev nechajte vyschnúť bez žehlenia a vysokej teploty.",
                "Preskúmajte polohu, povrch a farbu každej mapy.",
                "Porovnajte postihnuté miesto s vnútorným lemom.",
                "Pri podozrení na povlak urobte čistý oplach.",
                "Pri lokálnej škvrne otestujte vhodný predčistič.",
                "Po oprave upravte dávku, náplň a triedenie.",
                "Výsledok posúďte na suchom odeve pri dennom svetle.",
            ],
            "checks": [
                ["Poloha", "Je zmena v záhybe, na šve alebo na náhodnej ploche?"],
                ["Povrch", "Je stopa prášková, klzká, mastná alebo hladko vyblednutá?"],
                ["Reakcia", "Zmenila sa po čistom oplachu bez ďalšieho výrobku?"],
            ],
            "expert": [
                "Viditeľný vzhľad tmavého textilu tvorí pigment aj mikroštruktúra povrchu. Oder môže meniť rozptyl svetla bez toho, aby vznikla samostatná škvrna, zatiaľ čo minerálny alebo detergentný povlak pridáva na povrch inú vrstvu.",
                "Diagnostický postup preto začína reverzibilným testom s nízkym rizikom. Ďalšia chemická zmes bez identifikácie príčiny môže zmeniť farbu, poškodiť úpravu alebo zafixovať nečistotu.",
            ],
            "sources": [["GINETEX: symboly prania", GINETEX], ["Európska komisia: informácie o pracích cykloch", EU_WASHING]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Zmizne biela mapa po ďalšom praní?", "Ak ide o zvyšok produktu, čistý oplach môže pomôcť; vyblednutie alebo oder nezmiznú."],
                ["Mám pridať viac gélu?", "Nie. Pri povlaku by vyššia dávka mohla problém zhoršiť."],
                ["Ako rozoznať vyblednutie?", "Porovnajte exponované miesta s vnútorným lemom a sledujte, či oplach zmenu ovplyvní."],
                ["Môžem použiť ocot?", "Iba ak je kompatibilný s textilom, práčkou a ostatnými výrobkami; nikdy ho nemiešajte s chlórovým bielidlom."],
                ["Kedy použiť sušičku?", "Až keď je škvrna odstránená alebo je potvrdené, že nejde o zvyšok citlivý na teplo."],
            ],
        },
        {
            "post_id": "1906",
            "title": "Ako prať čierne oblečenie, aby nevybledlo",
            "slug": "ako-prat-cierne-oblecenie-aby-nevybledlo",
            "short": "Vyblednutie čierneho oblečenia spomaľuje triedenie, pranie naruby, nižšia povolená teplota, menšie trenie a šetrné sušenie.",
            "description": "Preventívny návod na čierne oblečenie: prvé pranie, triedenie, pranie naruby, teplota, otáčky, sušenie a ochrana pigmentu.",
            "quick": "Čierne oblečenie perte s podobnými tmavými farbami, naruby, pri najnižšej teplote, ktorá bezpečne zvládne znečistenie a ktorú povoľuje štítok. Obmedzte trenie, nepreplňte bubon, dávkujte presne a sušte mimo zbytočne vysokého tepla a dlhého ostrého slnka.",
            "intro": "Tento článok je preventívna rutina pred poškodením. Keď už je odev po praní fľakatý alebo má biele mapy, použite diagnostický návod; pri výbere produktu zas pomôže samostatný článok o géle na čierne.",
            "focus": "prvé cykly nového odevu, triedenie, pranie naruby, zníženie oderu, vhodnú teplotu, otáčky a sušenie",
            "boundary": "opravu už vzniknutých bielych máp a chemicko-funkčné porovnanie pracích gélov pre tmavú bielizeň",
            "points": [
                "prevencia chráni pigment a povrch, no už stratenú farbu neobnoví",
                "čierne kusy rozdeľujte aj podľa materiálu, hmotnosti a mechanickej odolnosti",
                "nižšia teplota pomáha iba vtedy, keď program stále odstráni nečistoty",
                "sušenie a svetlo sú rovnako dôležité ako samotný prací cyklus",
            ],
            "sections": [
                (
                    "Prvé pranie nového čierneho kusu",
                    [
                        "Nový odev môže uvoľňovať prebytočné farbivo. Skontrolujte etiketu, vnútorný lem a odporúčanie prať oddelene. Prvé cykly neriskujte so svetlým detailom alebo bielizňou, ktorú nemožno jednoducho opraviť.",
                        "Nenechávajte tmavý kus po praní mokrý v kontakte so svetlejším textilom. Vyberte ho bez čakania, vytvarujte a sušte podľa štítku. Prenos pigmentu môže pokračovať aj po skončení cyklu.",
                    ],
                ),
                (
                    "Ako znížiť mechanický oder",
                    [
                        "Oblečenie obráťte naruby, zapnite zipsy a hrubé kovové prvky oddeľte od jemných úpletov. Ťažké džínsy neperte s tenkou viskózou iba preto, že sú oba kusy čierne.",
                        "Zvoľte primerané otáčky a bubon neprepĺňajte. Veľmi prázdny bubon nie je automaticky šetrný, pretože kus môže padať s väčšou energiou; cieľom je vyvážená podobná náplň.",
                    ],
                ),
                (
                    "Teplota, program a dávka",
                    [
                        "Použite najnižšiu povolenú teplotu, ktorá zvládne konkrétne znečistenie. Krátky cyklus nemusí dobre odstrániť kožný maz ani opláchnuť nadmernú dávku. Farbu nechráni program, ktorý nechá vo vláknach nečistotu.",
                        "Gél odmerajte podľa etikety a tvrdej vody. Výberu sa venuje článok <a href=\"/n/praci-gel-na-cierne-oblecenie\">prací gél na čierne oblečenie</a>. Produkt je iba jedna časť rutiny; trenie a sušenie ostávajú rozhodujúce.",
                    ],
                ),
                (
                    "Sušenie, žehlenie a skladovanie",
                    [
                        "Tmavé kusy sušte naruby, ak to konštrukcia umožňuje, a nevystavujte ich zbytočne dlho ostrému slnku. Sušičku nastavte podľa štítku a ukončite cyklus po dosiahnutí suchosti, nie po presušení.",
                        "Pri žehlení rešpektujte bodky na symbole a citlivé lícne plochy žehlite naruby alebo cez ochrannú tkaninu. Do skrine ukladajte úplne suché kusy, aby sa vôňa nemiešala so zatuchnutím.",
                    ],
                ),
            ],
            "table": {
                "headers": ["Faktor", "Šetrnejšia voľba", "Riziko"],
                "rows": [
                    ["Triedenie", "podobný materiál a farba", "prenos a oder"],
                    ["Poloha", "naruby", "trená lícna strana"],
                    ["Teplota", "najnižšia účinná", "strata pigmentu"],
                    ["Sušenie", "primerané teplo a tieň", "presušenie a svetlo"],
                ],
            },
            "steps": [
                "Prečítajte štítok a nový nestály kus oddeľte.",
                "Roztrieďte čiernu náplň podľa materiálu a hmotnosti.",
                "Otočte odevy naruby a zabezpečte zipsy.",
                "Zvoľte najnižšiu účinnú povolenú teplotu.",
                "Odmerajte gél a nechajte bubnu primeraný priestor.",
                "Po cykle kusy okamžite vyberte a sušte šetrne.",
                "Stav farby porovnávajte dlhodobo na rovnakom mieste odevu.",
            ],
            "checks": [
                ["Nový kus", "Uvádza etiketa samostatné pranie alebo riziko púšťania farby?"],
                ["Trenie", "Nie sú v náplni ťažké zipsy a hrubé kusy s jemným úpletom?"],
                ["Teplo", "Používate iba teplotu a sušenie, ktoré textil skutočne potrebuje?"],
            ],
            "expert": [
                "Blednutie je súčet chemických a fyzikálnych zmien počas mnohých cyklov. Pigment môže migrovať do vody, povrch vlákna sa môže obrusovať a svetlo môže meniť farebný dojem; preto prevencia pracuje s celým životným cyklom odevu.",
                "Maximálna teplota na štítku nie je povinné nastavenie. GINETEX zároveň rozlišuje miernosť prania a sušenia, čo umožňuje vybrať šetrnejší proces pre tmavý materiál pri bežnom znečistení.",
            ],
            "sources": [["GINETEX: pranie, sušenie a žehlenie", GINETEX], ["Európska komisia: údaje o pracích programoch", EU_WASHING]],
            "commerce": LAUNDRY_COMMERCE,
            "faq": [
                ["Obnoví prací gél vyblednutú čiernu?", "Nie. Správna rutina môže spomaliť ďalšiu zmenu, ale stratený pigment nevráti."],
                ["Prečo prať naruby?", "Lícna strana je menej vystavená priamemu treniu o bubon a ostatné kusy."],
                ["Je 30 °C vždy najlepších?", "Nie. Zvoľte najnižšiu účinnú teplotu povolenú štítkom podľa materiálu a znečistenia."],
                ["Môžem čierne džínsy prať s jemnou blúzkou?", "Nie je to ideálne; rozdiel hmotnosti a povrchu zvyšuje mechanické zaťaženie."],
                ["Ako sušiť čierne oblečenie?", "Podľa štítku, bez zbytočne vysokého tepla a dlhého ostrého slnka."],
            ],
        },
    ]
)

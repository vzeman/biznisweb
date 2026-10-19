"""Reusable rich-HTML renderer for scoped VEVO article expansions."""

from __future__ import annotations

import html
import re
from typing import Any

import vevo_public_content_guard as public_guard


def normalize_admin_unicode(value: str) -> str:
    return (value or "").encode("utf-16", "surrogatepass").decode(
        "utf-16", "replace"
    )


def sanitize_legacy(markup: str) -> str:
    markup = normalize_admin_unicode(markup)
    markup = re.sub(r"(?is)<script\b[^>]*>.*?</script>", "", markup)
    markup = re.sub(r"(?is)<!--.*?-->", "", markup)
    markup = public_guard.sanitize_text(markup)
    markup = re.sub(
        r"(?i)\bCena\s*:",
        "Aktuálnu cenu nájdete na stránke produktu.",
        markup,
    )
    markup = re.sub(r"(?i)\b\d{1,4}[,.]\d{2}\s*(?:€|EUR)", "", markup)
    markup = re.sub(r"(?is)<(strong|span)\b[^>]*>\s*</\1>", "", markup)
    markup = re.sub(
        r"(?is)<p\b[^>]*>(.*?)</p>",
        lambda match: ""
        if len(
            html.unescape(re.sub(r"<[^>]+>", " ", match.group(1))).strip()
        )
        <= 2
        else match.group(0),
        markup,
    )
    return re.sub(r"[ \t]{2,}", " ", markup).strip()


def render_table(table: dict[str, Any]) -> str:
    headers = "".join(
        '<th style="border:1px solid #e5e5e5;padding:10px;text-align:left;">'
        f"{html.escape(str(value))}</th>"
        for value in table["headers"]
    )
    rows = []
    for row in table["rows"]:
        cells = "".join(
            '<td style="border:1px solid #e5e5e5;padding:10px;vertical-align:top;">'
            f"{html.escape(str(value))}</td>"
            for value in row
        )
        rows.append(f"<tr>{cells}</tr>")
    return (
        '<table style="width:100%;border-collapse:collapse;margin:20px 0;">'
        f"<thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    )


def render_deep_dive(config: dict[str, Any]) -> str:
    sections = []
    for title, paragraphs in config["deep_dive"]:
        sections.append(f"<h2>{html.escape(title)}</h2>")
        sections.extend(f"<p>{paragraph}</p>" for paragraph in paragraphs)
    return "".join(sections)


def render_decision_framework(config: dict[str, Any]) -> str:
    topic = html.escape(config["title"])
    points = [html.escape(value) for value in config["points"]]
    steps = [html.escape(value) for value in config["steps"]]
    checks = config["checks"]
    return f"""
<h2>Ako si vytvoriť spoľahlivý domáci postup</h2>
<p>Pri téme <strong>{topic}</strong> býva najväčšou chybou zmeniť niekoľko podmienok naraz a potom pripísať výsledok jedinej z nich. Najprv si preto určte konkrétny cieľ a východiskový stav. Kontrolný bod pre tento článok znie: {points[0]}. Pri prvom pokuse ponechajte ostatné podmienky čo najpodobnejšie bežnej rutine.</p>
<p>Začnite krokom: {steps[0]} Pokračujte: {steps[1]} Zapíšte si materiál alebo typ povrchu, množstvo výrobku, teplotu, čas, spôsob mechanického pôsobenia a stav po úplnom vysušení. Pri vôni pridajte čas hodnotenia, pretože prvý dojem nemusí zodpovedať výsledku o niekoľko hodín. Pri textílii oddeľte vzhľad a dotyk od funkcie, napríklad savosti, pružnosti či farebnej stálosti.</p>
<div style="border:1px solid #e1e6df;border-radius:8px;padding:18px;margin:22px 0;background:#fafcf9;">
<h3 style="margin-top:0;">Tri kontrolné otázky pred záverom</h3>
<ul>
<li><strong>{html.escape(checks[0][0])}:</strong> {html.escape(checks[0][1])}</li>
<li><strong>{html.escape(checks[1][0])}:</strong> {html.escape(checks[1][1])}</li>
<li><strong>{html.escape(checks[2][0])}:</strong> {html.escape(checks[2][1])}</li>
</ul>
</div>
<p>V druhom pokuse zmeňte iba jednu premennú a rešpektujte zásadu: {points[1]}. Ak výsledok nie je jednoznačný, zopakujte ho na porovnateľnej náplni alebo ploche. Tak odlíšite skutočný účinok od rozdielu spôsobeného tvrdosťou vody, mierou znečistenia, vlhkosťou, dávkou, vetraním alebo prirodzenou variabilitou čuchového vnemu.</p>
<p>Rozhodnutie urobte až po kroku: {steps[-1]} Zároveň platí: {points[2]}. Ak sa výsledok zhoršil, vráťte sa k poslednému funkčnému nastaveniu a skontrolujte etiketu výrobku, ošetrovací štítok textilu alebo návod spotrebiča. Ak je výsledok stabilne lepší, zapíšte si konkrétnu kombináciu podmienok. Získate opakovateľný postup namiesto všeobecnej rady bez znalosti materiálu a spôsobu použitia.</p>
""".strip()


def render_tracking_table(config: dict[str, Any]) -> str:
    return render_table(
        {
            "headers": ["Záznam", "Pred pokusom", "Po pokuse"],
            "rows": [
                ["Cieľ", config["points"][0], "splnený / nesplnený"],
                ["Nastavenie", config["steps"][1], "bez zmeny / upravené"],
                ["Kontrola", config["checks"][2][1], "výsledok a poznámka"],
                ["Ďalší krok", config["steps"][-1], "ponechať / zopakovať"],
            ],
        }
    )


def render_recovery_framework(config: dict[str, Any]) -> str:
    topic = html.escape(config["title"])
    points = [html.escape(value) for value in config["points"]]
    checks = config["checks"]
    steps = [html.escape(value) for value in config["steps"]]
    return f"""
<h2>Čo robiť, keď prvý výsledok nie je správny</h2>
<p>Pri téme <strong>{topic}</strong> oddeľte príčinu od následku. To, čo vidíte alebo cítite na konci, môže vzniknúť v príprave, počas hlavného procesu alebo až pri sušení a skladovaní. Začnite otázkou: {html.escape(checks[0][1])} Potom porovnajte stav s nepoužitou časťou, podobným kusom alebo malou kontrolnou plochou. Cieľom nie je okamžite pridať ďalší výrobok, ale nájsť krok, v ktorom sa výsledok odchýlil.</p>
<div style="border:1px solid #eadfce;border-radius:8px;padding:18px;margin:24px 0;background:#fffdf9;">
<h3 style="margin-top:0;">Poradie opravy s najnižším rizikom</h3>
<ol>
<li>Overte základnú podmienku: {points[0]}.</li>
<li>Skontrolujte druhú možnú odchýlku: {html.escape(checks[1][1])}</li>
<li>Zachovajte zásadu: {points[2]}.</li>
<li>Až potom zopakujte krok: {steps[-1]}</li>
</ol>
</div>
<p>Ak sa problém po šetrnej kontrole zmenší, pokračujte rovnakým smerom a upravujte iba jednu premennú. Ak zostane úplne rovnaký, neopakujte bez konca ten istý zásah. Presuňte sa k ďalšej hypotéze, skontrolujte materiál, povrch, spotrebič alebo podmienky prostredia. Pri nezvratnom opotrebovaní, poškodenej povrchovej úprave alebo rozpadajúcom sa materiáli môže ďalší cyklus stav zhoršiť.</p>
<p>Výsledok dokumentujte krátkou poznámkou alebo fotografiou v rovnakom svetle. Zapíšte dávku, čas, teplotu, náplň, použitý výrobok a spôsob sušenia. Poznačte aj to, čo ste zámerne ponechali bez zmeny, aby ďalšie porovnanie zostalo čitateľné. Tak rozlíšite jednorazovú odchýlku od opakovaného vzoru. Ak sa objaví zdravotné riziko, silné dráždenie, pleseň vo väčšom rozsahu, poškodená elektrická časť alebo neistota pri nebezpečnej chémii, domáci pokus ukončite a použite odborný postup.</p>
""".strip()


def render_expansion(config: dict[str, Any], marker: str) -> str:
    points = "".join(f"<li>{html.escape(value)}</li>" for value in config["points"])
    sections = []
    for title, paragraphs in config["sections"]:
        sections.append(f"<h2>{html.escape(title)}</h2>")
        sections.extend(f"<p>{paragraph}</p>" for paragraph in paragraphs)
    steps = "".join(f"<li>{html.escape(value)}</li>" for value in config["steps"])
    expert = "".join(f"<p>{value}</p>" for value in config["expert"])
    sources = "".join(
        (
            f'<li><a rel="noopener" href="{html.escape(url)}" target="_blank">'
            f"{html.escape(label)}</a></li>"
        )
        for label, url in config["sources"]
    )
    faq = "".join(
        f"<h3>{html.escape(question)}</h3><p>{html.escape(answer)}</p>"
        for question, answer in config["faq"]
    )
    commerce = config["commerce"]

    return f"""
<!-- {marker} -->
<p><strong>Rýchla odpoveď:</strong> {config['quick']}</p>
<p>{config['intro']}</p>
<div style="border:1px solid #e6ded2;border-radius:8px;padding:18px;margin:22px 0;background:#fffaf5;">
<h2 style="margin-top:0;">Presný záber tohto článku</h2>
<p><strong>Riešime:</strong> {config['focus']}</p>
<p><strong>Samostatne riešia iné návody:</strong> {config['boundary']}</p>
</div>
<div style="border:1px solid #d9e2ea;border-radius:8px;padding:18px;margin:22px 0;background:#f8fbfd;">
<h2 style="margin-top:0;">Najdôležitejšie body</h2>
<ul>{points}</ul>
</div>
{''.join(sections)}
{render_deep_dive(config)}
<h2>Porovnanie v skratke</h2>
{render_table(config['table'])}
<h2>Kontrolný záznam pre porovnateľný výsledok</h2>
{render_tracking_table(config)}
<div style="border:1px solid #d9e2ea;border-radius:8px;padding:18px;margin:24px 0;background:#f8fbfd;">
<h2 style="margin-top:0;">Praktický postup krok za krokom</h2>
<ol>{steps}</ol>
</div>
<h2>Odbornejší pohľad</h2>
{expert}
<div style="border:1px solid #e5e5e5;border-radius:8px;padding:16px;margin:20px 0;background:#fff;">
<h3 style="margin-top:0;">Použité odborné zdroje</h3>
<ul>{sources}</ul>
</div>
<div style="border:1px solid #dbe5de;border-radius:8px;padding:18px;margin:24px 0;background:#f7fbf8;">
<h2 style="margin-top:0;">{html.escape(commerce['category_title'])}</h2>
<p>{html.escape(commerce['category_body'])}</p>
<p><a style="display:inline-block;padding:11px 16px;border-radius:6px;border:1px solid #111;color:#111;text-decoration:none;" href="{html.escape(commerce['category_href'])}">Pozrieť kategóriu</a></p>
<div style="border:1px solid #e5e5e5;border-radius:8px;padding:16px;background:#fff;margin:14px 0;">
<h3 style="margin-top:0;">{html.escape(commerce['product_title'])}</h3>
<p>{html.escape(commerce['product_body'])}</p>
<p><a style="display:inline-block;padding:11px 16px;border-radius:6px;background:#111;color:#fff;text-decoration:none;" href="{html.escape(commerce['product_href'])}">Pozrieť produkt</a></p>
</div>
</div>
<h2>Najčastejšie otázky</h2>
{faq}
""".strip()


def append_preserving_original(
    config: dict[str, Any], existing_long: str, marker: str
) -> tuple[str, bool]:
    normalized = normalize_admin_unicode(existing_long)
    if marker in normalized:
        return normalized, True
    return (
        render_expansion(config, marker)
        + "\n<h2>Ďalší pôvodný prehľad témy</h2>\n"
        + sanitize_legacy(normalized),
        False,
    )

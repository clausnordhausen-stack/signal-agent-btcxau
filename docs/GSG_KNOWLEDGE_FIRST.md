# GSG Knowledge-first · TradingGuard – Signal-Engine und Risikomodell

**Verbindlicher Entwicklungsstandard (DE-Master, 11.10.2026).** Ziel: Bereits geprüftes GSG-Wissen vor neuer Arbeit wiederverwenden. Dieses Repository bleibt fachlich eigenständig; es erhält **keinen** separaten Wissenstresor, kein eigenständiges Bezahlsystem und keine administrativen Veröffentlichungsrechte.

## 1. Vor einer neuen Aufgabe: Wissen nachschlagen
Spezifiziere zunächst Problem, gewünschtes Ergebnis, beteiligte Datenquellen, Risiko und betroffene Version. Prüfe auf der **für jedermann öffentlich lesbaren** GSG-Wissensseite:
https://guardsystemsgroup.com/knowledge/development-insights

Für dieses Repository sind besonders relevant:
- `TradingGuard-WISSEN-001`: https://guardsystemsgroup.com/knowledge/development-insights#TradingGuard-WISSEN-001
- `TradingGuard-WISSEN-002`: https://guardsystemsgroup.com/knowledge/development-insights#TradingGuard-WISSEN-002

Lies die **gesamten Erklärungen** einschließlich Ausgangsproblem, Erkenntnis, Praxisbeispiel, Wissenstransfer und Grenzen – nicht nur Titel oder Status. Ein Artikel mit Kennzeichnung `concept` ist eine Designanforderung, kein nachgewiesenes produktives Feature. Prüfe, ob die Quelle und die fachliche Version noch zum aktuellen Problem passen.

## 2. Bereits gelöstes Problem wiederverwenden
Bei Score-, Risikogate- und Ausführungslogik vor Neuentwicklung TradingGuard-WISSEN-002 verwenden: ein Warnwert ist keine Freigabe, fehlende Risiko- oder Verbindungsdaten lösen keine stillschweigende Aktion aus. Zusätzlich TradingGuard-WISSEN-001 auf Aktualität von Snapshots und Status beachten.

Dokumentiere im PR den verwendeten Wissenseintrag. Ist die bisherige Lösung fachlich passend, verwende ihre geprüften Prinzipien und untersuche **nur** die verbleibenden Unterschiede. Kein unnötiger Neuaufbau, keine neue Recherche derselben Grundlagen und kein unverifizierter Copy/Paste-Transfer zwischen unterschiedlichen Produktkontexten.

## 3. Neues oder besseres Wissen vorschlagen
Zu jeder substanziellen Änderung gehört eine kurze Knowledge-first-Prüfung im PR: verwendete Artikel-ID, neue Evidenz/Tests, begründete Abweichungen und die künftig übertragbare Erkenntnis. Eine bessere, fachlich tiefere oder korrigierte Lösung wird **als neue Version zum bestehenden Thema** vorgeschlagen. Nur ein eigenständiges Problem rechtfertigt eine neue Wissens-ID.

Entwickler, Automationen und Agenten dürfen Kandidaten **dokumentieren und zur Freigabe vorschlagen**. **Nur die ausdrücklich konfigurierte aktive GSG-Knowledge-Owner-Identität darf die zentrale Wissensdatenbank ändern und veröffentlichen.** Andere GSG-Admins, Kunden oder Agenten erhalten dadurch keine Schreibrechte. Die öffentliche GSG-Wissensbibliothek ist für jeden ohne Anmeldung oder Kauf lesbar; interne Unterlagen bleiben streng getrennt.

## 4. Datenschutz, IP und Freigabe
Der konkrete aktuelle Betriebs-/Migrationsstatus dieses Repositories ist vor Änderungen gesondert zu verifizieren. Bestehende Risk Gates nicht lockern, keine Kunden- oder Brokerinformationen öffentlich kopieren und keine Anlageerfolgsgarantie behaupten.

Keine Tokens, private Schlüssel, Kundendossiers oder proprietären internen Quellcodes in öffentlich veröffentlichbare Texte aufnehmen. Ein Codepfad beweist nicht automatisch den Livebetrieb. Echte CI-Ergebnisse sowie gegebenenfalls Staging- und E2E-Nachweise protokollieren. Bestehende Kundensysteme und parallele PRs vor Änderungen auf Konflikte prüfen.

## 5. Maschinenlesbarer Index und Ressourceneffizienz
Nach **Freigabe und Veröffentlichung von GSG-Web-PR #51** existiert im GSG-Website-Repository der optionale, lokale Befehl:
`node scripts/knowledge-first.mjs search "konkrete Fachfrage"`.
Dieser durchsucht ausschließlich den öffentlichen Artikelbestand und liefert IDs, Fundstellen und Grenzen **ohne KI-Modell, API-Aufruf oder Owner-Token**. Solange der PR nicht freigegeben ist oder das Web-Checkout fehlt, nutze die oben verlinkte öffentliche Seite. Nie behaupten, die neue Suche sei bereits produktiv verfügbar.

Bei wiederholten Anfragen zuerst einen passenden Wissensartikel verwenden und nur tatsächliche Unterschiede neu bearbeiten. Wenn ein Artikel veraltet ist, eine überprüfbare Berichtigung vorschlagen; nicht ohne Owner-Freigabe öffentlich überschreiben. Keine erfundenen Token- oder Kosteneinsparungen behaupten.

**Querverweise:** GSG-WISSEN-GOV-002 und GSG-WISSEN-003 (DE-Master), GSG-Web-Issue #48, GSG-Web-PR #51. Der zentrale API-Wissensdienst in GSG-API-PR #213 ist bislang ein Draft und darf nicht von diesem Repository als aktiv vorausgesetzt werden.

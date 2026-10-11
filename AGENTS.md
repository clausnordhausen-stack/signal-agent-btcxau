# GSG Entwicklungsstandard · TradingGuard – Signal-Engine und Risikomodell

Vor jeder relevanten Änderung **zuerst bestehendes GSG-Wissen prüfen und wiederverwenden**. Maßgeblich ist `docs/GSG_KNOWLEDGE_FIRST.md` in diesem Repository, insbesondere die produktspezifischen Wissens-IDs und Schutzgrenzen.

Arbeitsfolge:
1. Im öffentlichen, ohne Login lesbaren GSG-Wissenszentrum nach dem konkreten Problem suchen. Titel **und** inhaltliche Grenzen vollständig prüfen. Bei verfügbarer freigegebener CLI nach Web-PR #51 die lokale `knowledge-first.mjs search` nutzen.
2. Vorhandene geprüfte Lösung übernehmen, wenn sie für Version, Datenquelle und Rechtslage passt. Neue Analyse auf wirkliche Abweichungen begrenzen. Keine ungültigen oder nur geplanten Funktionen als LIVE behaupten.
3. Neue Erkenntnis, Korrektur oder Vertiefung mit Problem, Beleg, Beispiel, Transfer, Grenzen und Referenz auf bestehende Knowledge-ID als **Vorschlag** im PR dokumentieren; keine isolierten konkurrierenden Wahrheiten erzeugen.
4. **Nur der einzige ausdrücklich konfigurierte aktive GSG Knowledge Owner darf die zentrale Datenbank bearbeiten, freigeben oder zurückziehen.** Agenten, sonstige Admins und Entwickler dürfen nicht dessen Authentifizierung verwenden. Jeder Mensch darf den freigegebenen öffentlichen Bestand anonym lesen.
5. Bestehende produktive Funktionen, Kundendaten, GSG-Zentralbilling, Verträge und parallele Branches vor Änderungen schützen. Keine Geheimnisse, internen Code oder geschützten personenbezogenen Daten in öffentliche Wissensbeiträge übertragen.
6. CI und echte Tests nennen, fehlende Runner/Abnahmen offen kennzeichnen. Nicht allein wegen dieser Datei eine nicht bestehende Backend-Integration behaupten.

Diese Datei ist eine dokumentierte Arbeitsanweisung. Durchsetzung mittels automatischer Repository-Gates wird erst nach isoliertem Test und Zustimmung zu den Releasebedingungen eingerichtet.

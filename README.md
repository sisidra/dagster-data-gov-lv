# Dagster integrācija datiem no data.gov.lv

!Neoficiāls projekts!

Dagster datu orķestrētājs [https://dagster.io/].

Droši var izmantot dažādiem pētniecības un izmēģinājumu projektiem par pamatu.

## Kā sākt?

Izmantojiet kādu vituālo Python vietu un instalējiet nepieciešamās bibliotēkas:

```bash
pip install -e ".[dev]"
```

Startējiet Dagster UI web server:

```bash
DAGSTER_HOME=$PWD/data dagster dev
```

Atveriet http://localhost:3000 pārlūkā.

## Brīdinājums

* Pirmā startēšanas reize var būt lēna, mēģiniet vairākkārt
* Tiek izmantots cache `data/data-gov-lv.cache`, kas šobrīd var izaugt līdz 10GB


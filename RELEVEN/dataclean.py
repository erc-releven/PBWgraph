import pbw
import config
import re
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine('mysql+pymysql://' + config.dbstring)
smaker = sessionmaker(bind=engine)
session = smaker()

srclist = {
    "Eustathios: Capture of Thessalonike": "Greek",
    "Kekaumenos": "Greek",
    "Attaleiates: History": "Greek",
    "Skylitzes": "Greek",
    "Skylitzes Continuatus": "Greek",
    "Synod of 1094": "Greek",
    "Theophylaktos of Ohrid, Letters": "Greek",
    "Boilas": "Greek",
    "Bryennios": "Greek",
    "Kinnamos": "Greek",
    "Synod of 1072": "Greek",
    "Chrysobull of 1079": "Greek",
    "Lazaros of Galesion": "Greek",
    "Glykas": "Greek",
    "Alexios Stoudites": "Greek",
    "Attaleiates: Diataxis": "Greek",
    "Kleinchroniken": "Greek",
    "Mauropous: Orations": "Greek",
    "Mauropous: Letters": "Greek",
    "Psellos": "Greek",
    "Pakourianos": "Greek",
    "Zonaras": "Greek",
    "Psellos: Chronographia": "Greek",
    "Eleousa: Typikon": "Greek",
    "Kyrillos Phileotes": "Greek",
    "Patmos: Acts": "Greek",
    "Lavra": "Greek",
    "Patmos: Typikon": "Greek",
    "Patmos: Codicil": "Greek",
    "Eleousa: Acts": "Greek",
    "Patmos: Testament": "Greek",
    "Yahya al-Antaki": "Arabic",
    "William of Tyre": "Latin",
    "Anna Komnene": "Greek",
    "Hypomnema on marriage": "Greek",
    "Eustathios Romaios": "Greek",
    "Aristakes": "Armenian",
    "Dionysiou": "Greek",
    "Kastamonitou": "Greek",
    "Xenophontos": "Greek",
    "Esphigmenou": "Greek",
    "Docheiariou": "Greek",
    "Protaton": "Greek",
    "Hilandar": "Greek",
    "Vatopedi": "Greek",
    "Nea Mone": "Greek",
    "Oath of Eudokia": "Greek",
    "Panteleemon": "Greek",
    "Ioannes Italos": "Greek",
    "Pantokrator (Athos)": "Greek",
    "Iveron": "Greek",
    "Xeropotamou": "Greek",
    "Nea Mone,": "Greek",
    "Annales Barenses": "Latin",
    "Lupus protospatharius": "Latin",
    "Anonymus Barensis": "Latin",
    "Thebes: Cadaster": "Greek",
    "Thebes: Confraternity": "Greek",
    "Christophoros of Mitylene": "Greek",
    "Albert of Aachen": "Latin",
    "Domenico of Grado": "Latin",
    "Matthew of Edessa": "Armenian",
    "Humbert, Commemoratio": "Latin",
    "Humbert, Excommunication": "Latin",
    "Various sources": "",
    "Keroularios  ": "Greek",
    "Ibn al-Athir": "Arabic",
    "Leon of Ohrid (Greek)": "Greek",
    "Michael the Rhetor, Regel": "Greek",
    "Petros of Antioch, ep. 2": "Greek",
    "Tzetzes, Exegesis of Homer": "Greek",
    "Edict on Clergy Reform": "Greek",
    "Tzetzes, Posthomerica": "Greek",
    "Walter the Chancellor": "Latin",
    "Tzetzes, Homerica": "Greek",
    "Synodal edict (1054)": "Greek",
    "Leon of Ohrid (Latin)": "Latin",
    "Tzetzes, Historiai": "Greek",
    "Leo IX  ": "Latin",
    "Niketas Choniates, Historia": "Greek",
    "Prodromos, Historische Gedichte": "Greek",
    "Italikos": "Greek",
    "Tornikes, Georgios": "Greek",
    "Basilakios, Orationes et epistulae": "Greek",
    "Pantokrator Typikon": "Greek",
    "Council of 1147": "Greek",
    "Tzetzes, Letters": "Greek",
    "Council of 1157": "Greek",
    "Christos Philanthropos, note": "Greek",
    "Kecharitomene": "Greek",
    "Sakkos (1166)": "Greek",
    "Sakkos (1170)": "Greek",
    "Droit matrimonial": "Greek",
    "Manganeios Prodromos": "Greek",
    "Usama": "Arabic",
    "Ibn Shaddad": "Arabic",
    "Manasses, Chronicle": "Greek",
    "Synopsis Chronike": "Greek",
    "Manasses, Chronicle: Dedication": "Greek",
    "Fulcher of Chartres": "Latin",
    "Italos trial": "Greek",
    "Edict on clergy reform": "Greek",
    "Theophylact of Ohrid, Speech to Alexios I": "Greek",
    "Basileios of Calabria to Nikolaos III": "",
    "Clement III to Basileios of Calabria": "",
    "Synodal protocol (1089)": "Greek",
    "Nikolaos III to Urban II": "",
    "Victor (pope)": "Latin",
    "Gregory VII, in Caspar": "Latin",
    "Gregory VII, Epistolae vagantes": "Latin",
    "Nicolas d'Andida": "",
    "Documents d'ecclesiologie ": "Greek",
    "Leon of Chalcedon": "Greek",
    "Jus Graeco-Romanum, III": "Greek",
    "Semeioma on Leon of Chalcedon": "Greek",
    "Nicole, Chartophylax": "Greek",
    "Petros of Antioch  ": "Greek",
    "Manasses, Hodoiporikon": "Greek",
    "Hypomnesis of May, 1094": "Greek",
    "Paschal II, Letter to Alexios I": "Latin",
    "Various sources (see main entry)": "",
    "Humbert, Dialogus": "Latin",
    "Michel, Amalfi": "",
    "Zetounion": "Greek",
    "Peri metatheseon": "Greek",
    "Parthenon inscriptions": "Greek",
    "Niketas Stethatos (Darrouzes)": "Greek",
    "Niketas Stethatos, On the Holy Spirit": "Greek",
    "Odo of Deuil": "Latin",
    "Ralph of Caen": "Latin"
}

languages = {}
for lang in session.query(pbw.OrigLangAuth).all():
    languages[lang.oLanguage] = lang.oLangKey

for factoid in session.query(pbw.Factoid).all():
    if factoid.source == "Seals":
        continue
    explang = srclist.get(factoid.source)
    if explang == "":
        explang = "(Unspecified)"
    faclang = factoid.origLang

    # # Check what is in origLDesc and see if it makes sense
    # ostr = factoid.origLDesc
    # if ostr is not None and ostr != "":
    #     if re.search(r'[Α-Ωα-ω]+', ostr) is not None:
    #         # It's Greek and should be set as such
    #         explang = "Greek"
    #     elif re.search(r'[Ա-Քա-ք]+', ostr) is not None:
    #         explang = "Armenian"
    #     elif re.search(r'[\u0620-\u06d1]+', ostr) is not None:
    #         explang = "Arabic"
    #     # elif re.search(r'[A-Za-z]+', ostr) is not None:
    #     #     explang = "Latin"

    if faclang != explang and factoid.origLDesc != "":
        print("Language mismatch factoid %d, source %s: %s vs. %s" % (
            factoid.factoidKey, factoid.source, explang, faclang))
        # factoid.oLangKey = languages.get(explang)
# session.commit()
print("Done")
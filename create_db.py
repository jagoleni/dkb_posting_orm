import sys
import argparse

import pandas as pd

from sqlalchemy import (
    Column,
    String,
    DateTime,
    UniqueConstraint,
    Integer,
    Float,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


Base = declarative_base()


class BuchungGiro(Base):
    __tablename__ = "buchung_giro"

    id = Column(Integer, primary_key=True, autoincrement=True)
    buchungstag = Column(DateTime, nullable=False)
    wertstellung = Column(DateTime, nullable=False)
    buchungstext = Column(String)
    auftraggeber = Column(String)
    verwendungszweck = Column(String)
    kontonummer = Column(Integer, nullable=False)
    blz = Column(Integer, nullable=False)
    betrag = Column(Float, nullable=False)
    glaeubiger_id = Column(String)
    mandatsreferenz = Column(String)
    kundenreferenz = Column(String)

    __table_args__ = (
        UniqueConstraint("buchungstag", "verwendungszweck", "auftraggeber", "betrag"),
    )

    def __repr__(self) -> str:
        return "<BuchungGiro `{}` `{}` `{}` `{}`)>".format(
            self.auftraggeber, self.verwendungszweck, self.buchungstag, self.betrag
        )


def insert_dataframe_into_db(session, df):
    for idx, row in df.iterrows():
        buchung = BuchungGiro(
            buchungstag=row.Buchungstag,
            wertstellung=row.Wertstellung,
            buchungstext=row.Buchungstext,
            auftraggeber=row["Auftraggeber / Begünstigter"],
            verwendungszweck=row.Verwendungszweck,
            kontonummer=row.Kontonummer,
            blz=row.BLZ,
            betrag=float(row["Betrag (EUR)"].replace(".", "").replace(",", ".")),
            glaeubiger_id=row["Gläubiger-ID"],
            mandatsreferenz=row.Mandatsreferenz,
            kundenreferenz=row.Kundenreferenz,
        )
        session.add(buchung)
    session.commit()


def read_giro_export(fpath):
    return pd.read_csv(
        fpath,
        skiprows=5,
        encoding="latin",
        sep=";",
        parse_dates=["Buchungstag", "Wertstellung"],
        infer_datetime_format=True,
    )


def parse_cmdline_args():
    p = argparse.ArgumentParser()
    p.add_argument(
        "-o",
        "--output",
        nargs="?",
        default="dkb",
        help=("DB filneame. Default-name is dkb (saved to working_dir/name.db)"),
    )
    p.add_argument(
        "input_csv",
        type=str,
        metavar="C",
        help=("Name of the file you want to write into a DB"),
    )
    return p.parse_args()


def main():
    args = parse_cmdline_args()
    data = read_giro_export(args.input_csv)
    engine = create_engine("sqlite:///{}.db".format(args.output))
    Session = sessionmaker()
    Base.metadata.create_all(engine)
    Session.configure(bind=engine)
    session = Session()
    insert_dataframe_into_db(session, data)


if __name__ == "__main__":
    main()

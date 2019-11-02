import pbw
import config
import re
import convertdate
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def julian_day(year, month, day):
    """Return the Julian day for the given system"""
    return convertdate.julian.to_jd(year, month, day)


def julian_eom(year, month):
    """Return the Julian day for the end of the given month"""
    if month in convertdate.julian.HAVE_31_DAYS:
        return convertdate.julian.to_jd(year, month, 31)
    elif month in convertdate.julian.HAVE_30_DAYS:
        return convertdate.julian.to_jd(year, month, 30)
    elif convertdate.julian.leap(year):
        return convertdate.julian.to_jd(year, month, 29)
    else:
        return convertdate.julian.to_jd(year, month, 28)


def produce_range(datestr):
    """Produce a pair of strings where a dash has been decomposed"""
    # Regex all the ones that don't register algorithmically
    splitstr = re.match(r'(\d+)\s+(\w+)-(\d+)', datestr)
    if splitstr is not None:  # 1083 Winter-1085
        return "%s %s" % (splitstr.group(1), splitstr.group(2)), splitstr.group(3)
    splitstr = re.match(r'(\d+)-(\d+)\s+(\w+)-(\w+)', datestr)
    if splitstr is not None:  # 1046-1047 December-January
        return "%s %s" % (splitstr.group(1), splitstr.group(3)), "%s %s" % (splitstr.group(2), splitstr.group(4))
    splitstr = re.match(r'(\d+)\s+(\w+\s+\d+)-(\w+\s+\d+)', datestr)
    if splitstr is not None:  # 1033 February 20-March 15
        return "%s %s" % (splitstr.group(1), splitstr.group(2)), "%s %s" % (splitstr.group(1), splitstr.group(3))
    splitstr = re.match(r'(\d+)\s+(\w+(\s+\d+)?) to (\w+(\s+\d+)?)', datestr)
    if splitstr is not None:  # 1168 October to November 3
        return "%s %s" % (splitstr.group(1), splitstr.group(2)), "%s %s" % (splitstr.group(1), splitstr.group(4))
    # Now the algorithm.
    # First split the thing into space-separated words
    words = datestr.split()
    # Now find the index of the word with a dash
    rng = []
    for ridx in range(len(words)):
        if '-' in words[ridx]:
            # Split it
            parts = words[ridx].split('-')
            for p in parts:
                lst = words[0:ridx]
                lst.append(p)
                rng.append(' '.join(lst))
    return tuple(rng)


def day_string(jd):
    # Get the Julian calendar date
    moy = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    j = convertdate.julian.from_jd(jd)
    return "%d %s %d" % (j[2], moy[j[1]], j[0])


def parse_date(datestr):
    # Make a dating node. First try to parse the date
    dt = None
    dmin = None
    dmax = None
    try:  # 1085
        dt = datetime.strptime(datestr, "%Y")
        dmin = julian_day(dt.year, dt.month, dt.day)
        dmax = julian_day(dt.year, 12, 31)
    except ValueError:
        pass
    if dt is None:
        try:  # 1085 January
            dt = datetime.strptime(datestr, "%Y %B")
            dmin = julian_day(dt.year, dt.month, dt.day)
            dmax = julian_eom(dt.year, dt.month)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 Jan
            dt = datetime.strptime(datestr, "%Y %b")
            dmin = julian_day(dt.year, dt.month, dt.day)
            dmax = julian_eom(dt.year, dt.month)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 mid-January
            dt = datetime.strptime(datestr, "%Y mid-%B")
            dmin = julian_day(dt.year, dt.month, 9)
            dmax = julian_day(dt.year, dt.month, 21)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 middle of January
            dt = datetime.strptime(datestr, "%Y middle of %B")
            dmin = julian_day(dt.year, dt.month, 9)
            dmax = julian_day(dt.year, dt.month, 21)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 January 23
            dt = datetime.strptime(datestr, "%Y %B %d")
            dmin = julian_day(dt.year, dt.month, dt.day)
            dmax = julian_day(dt.year, dt.month, dt.day)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 23 January
            dt = datetime.strptime(datestr, "%Y %d %B")
            dmin = julian_day(dt.year, dt.month, dt.day)
            dmax = julian_day(dt.year, dt.month, dt.day)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 January23
            dt = datetime.strptime(datestr, "%Y %B%d")
            dmin = julian_day(dt.year, dt.month, dt.day)
            dmax = julian_day(dt.year, dt.month, dt.day)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 Jan 23
            dt = datetime.strptime(datestr, "%Y %b %d")
            dmin = julian_day(dt.year, dt.month, dt.day)
            dmax = julian_day(dt.year, dt.month, dt.day)
        except ValueError:
            pass
    if dt is None:
        try:  # 1085 after January
            dt = datetime.strptime(datestr, "%Y after %B")
            dmin = julian_day(dt.year, dt.month + 1, 1)
            dmax = julian_day(dt.year, 12, 31)
        except ValueError:
            pass
    if dt is None:
        try:  # c. 1085
            dt = datetime.strptime(datestr, "c. %Y")
            dmin = julian_day(dt.year - 3, 1, 1)
            dmax = julian_day(dt.year + 3, 12, 31)
        except ValueError:
            pass
    if dt is None:
        timeofday = re.match(r'(.*?),?\s+(morning|evening)$', datestr)
        if timeofday is not None:
            return parse_date(timeofday.group(1))
    if dt is None:
        easterdate = re.match(r'(\d+)\s+easter', datestr, flags=re.I)
        if easterdate is not None:
            # Get the date for Orthodox Easter that year
            dt = convertdate.holidays.easter(int(easterdate.group(1)), church="orthodox")
            dmin = julian_day(*dt)
            dmax = dmin
            print("Easter for year %d falls on %d/%d" % dt)
    if dt is None:
        lentdate = re.match(r'(\d+)\s+lent', datestr, flags=re.I)
        if lentdate is not None:
            # Get the date for Orthodox Easter that year
            dt = convertdate.holidays.easter(int(lentdate.group(1)), church="orthodox")
            dmax = julian_day(*dt) - 1
            dmin = dmax - 46
    if dt is None:
        pentdate = re.match(r'(\d+)\s+pentecost', datestr, flags=re.I)
        if pentdate is not None:
            # Get the date for Orthodox Easter that year
            dt = convertdate.holidays.easter(int(pentdate.group(1)), church="orthodox")
            dmin = julian_day(*dt) + 50
            dmax = dmin
    if dt is None and '-' in datestr or ' to ' in datestr:
        # See if we can parse two ends of a range.
        daterange = produce_range(datestr)
        if len(daterange) == 2:
            firstrange = parse_date(daterange[0])
            secondrange = parse_date(daterange[1])
            dt = firstrange[0]
            dmin = dt
            dmax = secondrange[1]
            # if dmin and dmax:
            #     print("Parsed string %s as %s - %s" % (datestr, day_string(dmin), day_string(dmax)))
    # Handle interstitial qualifiers
    if dt is None:
        beforeafter = re.match(r'(\d+)\s+(before|after|early|late|around|mid(dle)?|end)( of)?\s+(.*)$', datestr, flags=re.I)
        if beforeafter is not None:
            year = int(beforeafter.group(1))
            qualifier = beforeafter.group(2)
            rest = beforeafter.group(5)
            dt = parse_date("%d %s" % (year, rest))
            if dt[0] is not None:
                if qualifier.lower() == "before":
                    dmin = julian_day(year, 1, 1)
                    dmax = dt[0] - 1
                elif qualifier.lower() == "after":
                    dmin = dt[1] + 1
                    dmax = julian_day(year, 12, 31)
                elif qualifier.lower() == "early":
                    dmin = dt[0]
                    dmax = dt[0] + (dt[1] - dt[0]) / 2
                elif qualifier.lower() == "late":
                    dmin = dt[0] + (dt[1] - dt[0]) / 2
                    dmax = dt[1]
                elif qualifier.lower() == "around":
                    magnitude = dt[1] - dt[0]
                    if magnitude < 10:
                        # Add 3 days each side if it's a matter of days
                        dmin = dt[0] - 3
                        dmax = dt[1] + 3
                    else:  # Add half the timespan
                        dmin = dt[0] - magnitude / 2
                        dmax = dt[1] + magnitude / 2
                elif "mid" in qualifier.lower():
                    # Take the middle half of the timespan
                    magnitude = dt[1] - dt[0]
                    dmin = dt[0] + magnitude / 4
                    dmax = dt[1] - magnitude / 4
                elif qualifier.lower() == "end":
                    # Take the last 25% of the timespan
                    magnitude = dt[1] - dt[0]
                    dmin = dt[1] - magnitude / 4
                    dmax = dt[1]
                # print("Parsed interstitial %s as %s - %s" % (datestr, day_string(dmin), day_string(dmax)))
    if dt is None:
        inverted = False
        seasonstring = r'(winter|spring|summer|autumn|beginning|early|mid(dle)?|late|end|(first|second) half)'
        seasonal = re.match(r'(\d+)\s+%s$' % seasonstring, datestr.lower())
        if seasonal is None:
            inverted = True
            seasonal = re.match(r'%s\s+(\d+)$' % seasonstring, datestr.lower())
        if seasonal is not None:
            year = int(seasonal.group(4 if inverted else 1))
            season = seasonal.group(1 if inverted else 2)
            if season == "winter":
                dmin = julian_day(year-1, 12, 1)
                dmax = julian_day(year, 3, 20)
            elif season == "spring":
                dmin = julian_day(year, 3, 1)
                dmax = julian_day(year, 6, 20)
            elif season == "summer":
                dmin = julian_day(year, 6, 1)
                dmax = julian_day(year, 9, 20)
            elif season == "autumn":
                dmin = julian_day(year, 9, 1)
                dmax = julian_day(year, 12, 20)
            elif season == "beginning":
                dmin = julian_day(year, 1, 1)
                dmax = julian_eom(year, 2)
            elif season == "early":
                dmin = julian_day(year, 1, 1)
                dmax = julian_day(year, 4, 15)
            elif "mid" in season:
                dmin = julian_day(year, 4, 15)
                dmax = julian_day(year, 9, 15)
            elif season == "late":
                dmin = julian_day(year, 9, 15)
                dmax = julian_day(year, 12, 31)
            elif season == "end":
                dmin = julian_day(year, 11, 1)
                dmax = julian_day(year, 12, 31)
            elif season == "first half":
                dmin = julian_day(year, 1, 1)
                dmax = julian_day(year, 6, 30)
            elif season == "second half":
                dmin = julian_day(year, 7, 1)
                dmax = julian_day(year, 12, 31)
    return dmin, dmax


def clean_datestring(dstr):
    # Get rid of leading space and leading asterisk
    datestr = dstr.lstrip().lstrip('*')
    datestr = datestr.rstrip('*')
    # Get rid of em- or en-dashes
    datestr = datestr.replace('–', '-').replace('—', '-')
    # Get rid of trailing colons or spaces, condense all spaces to a single space
    datestr = re.sub(r'\s+', ' ', re.sub(r':?\s*$', '', datestr)).replace(' - ', '-')
    # Get rid of commas and colons after the year
    datestr = re.sub(r'^(\d+)[,:;]', r'\1', datestr)
    return datestr


def parse_date_info(nunit):
    # Get the date string and try to rationalise its format
    datestr = clean_datestring(nunit.dates)
    if not datestr or datestr == "0":
        return
    # Get the date type
    datetype = nunit.dateTypeKey  # 0/1 undef, 2 internal approx., 3 inferred, 4 uncertain, 5 median, 6 wrong
    # See if the date has some sort of qualifier
    qualified = re.match(r'(.*)\s+\((.*)\)\s*$', datestr)
    if qualified is None:
        qualified = re.match(r'(.*?)\s*\(?(\?)\)?(.*)$', datestr)
    if qualified is not None:
        datestr = qualified.group(1)
        qualifier = qualified.group(2).lower()
        if len(qualified.groups()) == 3:
            datestr += qualified.group(3)
        # Send it through the cleaner again just in case there is a leftover comma
        datestr = clean_datestring(datestr)
        if 'uncertain' in qualifier or 'uncetain' in qualifier or 'uncertan' in qualifier or qualifier == '?':
            datetype = 4
        elif 'mistaken' in qualifier or 'guess' in qualifier:
            datetype = 6
        elif 'median' in qualifier:
            datetype = 5
        elif 'august' in qualifier:
            # Singleton: after Michael VI accession in 1056
            datetype = 2
            datestr = "1056 autumn"
        elif qualifier == 'confused':
            # Singleton: event seems to have happened in 1052
            datestr = "1052"
        else:
            print("Ignoring date qualifier %s" % qualifier)
    # Make a dating node. First try to parse the date
    dmin, dmax = parse_date(datestr)
    if dmin is None or dmax is None:
        # Print the date in some kind of standardised format
        # print("Parsed date %s of type %d in range %s - %s"
        #       % (nunit.dates, datetype, day_string(dmin), day_string(dmax)))
        print("Unparsed date %s (%s)" % (nunit.dates, datestr))
    return dmin, dmax

if __name__ == '__main__':
    # Connect to the SQL DB
    engine = create_engine('mysql+pymysql://' + config.dbstring)
    smaker = sessionmaker(bind=engine)
    mysqlsession = smaker()
    # Get all the narrative dates
    unparsed = 0
    for nu in mysqlsession.query(pbw.NarrativeUnit).all():
        result = parse_date_info(nu)
        if result is not None and result[0] is None:
            unparsed += 1
    print("Total unparsed: %d" % unparsed)

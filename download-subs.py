#!/usr/bin/env python

import sys, os, re
reload(sys)
sys.setdefaultencoding('utf8') # problem with encoding

import argparse

import logging as log

import subliminal

from   babelfish import Language
from   datetime  import timedelta

import csv
import shutil


# Helpers
def search_video_files(rootdir, max_age=7, embedded_subs=False): #{{{
    '''Return a list of video files in rootdir which does not have any subtitle.

    Parameters
    ----------
    rootdir: str
        Root folder path from where to start the search.

    Returns
    -------
    list of subliminal.video
    '''
    dirpath = unicode(rootdir)

    # data path
    if not os.path.exists(dirpath):
        raise IOError('Folder {} not found.'.format(dirpath))

    # scan for videos in the folder and their subtitles
    videos = subliminal.scan_videos(dirpath)

    return videos
#}}}


def download_subs(videos, languages, dir_to_save="tmp"): #{{{
    """
    Will scan for videos newer than one week and try to download
    subtitles in English for them.

    Parameters
    ----------
    videos: list of

    languages: list of babelfish.language

    """
    nu_vids = []
    for vid in videos:
        if len(vid.subtitle_languages) == 0:
            nu_vids.append(vid)

    # download
    subs = subliminal.download_best_subtitles(nu_vids, languages)

    log.info('Subs found:')
    log.info(subs)

    # save
    log.debug('Saving subtitle files.')
    for v in videos:
        subliminal.save_subtitles(v, subs[v], directory=dir_to_save)
    # subliminal.save_subtitles(subs, single=False)
#}}}


def setup_log(filename='', verbose=False): #{{{
    """ Set up the logging system.

    Parameters
    ----------
    filename: str
        Path to a logging file. If empty, will print the log to sys.stderr.

    verbose: bool
        If True will turn on verbose mode.
    """
    log_level = log.ERROR
    if verbose == 2:
        log_level = log.DEBUG
    elif verbose == 1:
        log_level = log.INFO
    log_args = {'level': log_level}

    if filename:
        log_args['filename'] = filename

    log.basicConfig(**log_args)
#}}}


def setup_argparser(): #{{{
    """ Define and return the command argument parser. """
    parser = argparse.ArgumentParser(description='Video subtitle downloader.')

    script_path = os.path.abspath(sys.argv[0])
    default_filepath = os.path.dirname(script_path)+"/ml-latest-small/movies.csv"
    default_outpath  = os.path.dirname(script_path)+"/subtitles"

    parser.add_argument('-f', '--file', dest='filepath', required=False,
                        default=default_filepath, action='store',
                        help='''File containing a name list of films to be
                        download.
                        ''')

    parser.add_argument('-l', '--lang', dest='langs', required=False,
                        action='append', default=['eng'],
                        help='''Subtitle languages to look for.
                             Many can be specified. Use 3-letter code (ISO-639-3).
                             ''')

    parser.add_argument('--limit_movies', dest='limit_movies', required=False,
                        action='store', default=None, type=int,
                        help='''Number maximum of subtitles to download,
                        downloaded by movieId''')

    parser.add_argument('-o', '--outpath', dest='outpath', required=False,
                        action='store', default=default_outpath,
                        help='Folder to save subtitles.')

    parser.add_argument('--log', dest='logfile', required=False,
                        action='store', default="DL-subs.log",
                        help='Log file path.')

    parser.add_argument('-v', '--verbose', dest='verbose', required=False,
                        default=0, type=int,
                        help='Verbose level: 0 for errors, 1 for info or 2 for debug.')

    return parser
#}}}


def main(argv=None):

    parser = setup_argparser()

    try:
        args = parser.parse_args()
    except argparse.ArgumentError as exc:
        log.exception('Error parsing options.')
        parser.error(str(exc.message))
        raise

    verbose      = args.verbose
    logfile      = args.logfile
    filepath     = args.filepath
    outpath      = args.outpath
    langs        = args.langs
    limit_movies = args.limit_movies

    setup_log(logfile, verbose)
    log.info('{0} {1}'.format(os.path.basename(__file__), locals()))

    # Languages
    languages = {Language(lang) for lang in langs}

    # Paths
    script_path = os.path.abspath(sys.argv[0])

    tmp_path = os.path.dirname(script_path)+"/tmp"
    if not os.path.exists(tmp_path):
        os.makedirs(tmp_path)

    if not os.path.exists(outpath):
        os.makedirs(outpath)

    try:
        with open(filepath, 'r') as csvfile:
            csv_content = csv.reader(csvfile, delimiter=str(","))
            namelist = []
            for row in csv_content:
                namelist.append(row[1])
            if limit_movies is not None :
                namelist = namelist[:limit_movies+1]
    except:
        log.exception('Error occured when reading {}'.format(filepath))

    # Removing Token name :
    namelist.pop(0)

    # Creating .mp4 files :
    # CF : https://grouplens.org/datasets/movielens/
    for e in namelist:
        e = e.replace("/","")
        path_mp4 = tmp_path + "/" + e + ".mp4"
        open(path_mp4, 'w').close()
        print "e = ", e

    print "Found ", len(namelist), " movies subtitles to download ..."

    try:
        log.info('Looking for video files in folder {} for {}.'.format(tmp_path, languages))
        videos = search_video_files(tmp_path)
    except:
        log.exception('Error when searching for video files.')
        raise

    log.info('Videos found:')
    log.info(videos)

    try:
        download_subs(videos, languages, dir_to_save=outpath)
    except:
        log.exception('Error when downloading subtitles.')
        raise

    # Delete tmp directory
    shutil.rmtree(tmp_path, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())

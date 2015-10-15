#!/usr/bin/env python

from textwrap import wrap


NO_ERRORS_EXIT = 0
ERRORS_EXIT = 1
WARNINGS_EXIT = 2
CAN_NOT_OPEN_EVENT_FILE = 11
CAN_NOT_OPEN_CONF_FILE = 12
CAN_NOT_OPEN_CLUSTER_DB_FILE = 13
CAN_NOT_OPEN_PBS = 14
UNDEFINED_EVENT = 15
UNEXPECTED_ERROR = 64

def format_msg(msg_tmpl, extra, indent=8, width=64):
    indent = ' '*indent
    msg = '\n'.join([indent + x for x in
                     wrap(msg_tmpl.format(**extra), width=width)])
    return msg

if __name__ == '__main__':
    from argparse import ArgumentParser
    import json
    import os
    import sqlite3
    import sys
    import traceback
    from vsc.pbs.script_parser import PbsScriptParser
    from vsc.pbs.check import JobChecker, ScriptChecker

    arg_parser = ArgumentParser(description='PBS script syntax checker')
    arg_parser.add_argument('pbs_file', help='PBS file to check')
    arg_parser.add_argument('--conf', default='config.json',
                            help='configuration file')
    arg_parser.add_argument('--db', help='cluster database file')
    arg_parser.add_argument('--events', help='events file')
    arg_parser.add_argument('--quiet', action='store_true',
                            help='do not show summary')
    arg_parser.add_argument('--warnings_as_errors', action='store_true',
                            help='non zero exit code on warnings')
    arg_parser.add_argument('--show_job', action='store_true',
                            help='show job parameters')
    arg_parser.add_argument('--debug', action='store_true',
                            help='show debugging information for qlint '
                                 'internal errors')
    options, rest = arg_parser.parse_known_args()
    try:
        with open(options.conf, 'r') as conf_file:
            conf = json.load(conf_file)
    except EnvironmentError as error:
        msg = "### error: can not open configuration file '{0}'\n"
        sys.stderr.write(msg.format(options.conf))
        sys.exit(CAN_NOT_OPEN_CONF_FILE)
    if options.db:
        conf['cluster_db'] = options.db
    if options.events:
        conf['event_file'] = options.events
    if not os.path.isfile(conf['cluster_db']):
        msg = "### error: can not open cluster DB '{0}'\n"
        sys.stderr.write(msg.format(conf['cluster_db']))
        sys.exit(CAN_NOT_OPEN_CLUSTER_DB_FILE)
    try:
        with open(conf['event_file']) as event_file:
            event_defs = json.load(event_file)
    except EnvironmentError as error:
        msg = "### error: can not open event file '{0}'\n"
        sys.stderr.write(msg.format(conf['event_file']))
        sys.exit(CAN_NOT_OPEN_EVENT_FILE)
    indent = ' '*conf['report_indent']
    try:
        pbs_parser = PbsScriptParser(conf, event_defs)
        try:
            with open(options.pbs_file, 'r') as pbs_file:
                pbs_parser.parse_file(pbs_file)
        except EnvironmentError as error:
            msg = "### error: can not open PBS file '{0}'\n"
            sys.stderr.write(msg.format(options.events))
            sys.exit(CAN_NOT_OPEN_PBS)
        job_checker = JobChecker(conf, event_defs)
        job_checker.check(pbs_parser.job)
        pbs_parser.context = 'semantics'
        pbs_parser.merge_events(job_checker.events)
        script_checker = ScriptChecker(conf, event_defs)
        script_checker.check(pbs_parser.job,
                             pbs_parser.script_first_line_nr)
        pbs_parser.context = 'file'
        pbs_parser.merge_events(script_checker.events)
    except Exception as exception:
        msg = ('### error: qlint crashed unexpectedly with exception\n'
               '#          "{0}"\n'
               '#   If you want to help resolving this issue, please\n'
               '#   report it to: geertjan.bex@uhasselt.be\n'
               '#   Please include:\n'
               '#     * the PBS you were checking, and\n'
               '#     * the command line options qlint was invoked with.\n'
               '#   Apologies and thanks for your cooperation.\n')
        sys.stderr.write(msg.format(exception))
        if (options.debug):
            traceback.print_exc(file=sys.stderr)
        sys.exit(UNEXPECTED_ERROR)
    nr_warnings = 0
    nr_errors = 0
    for event in pbs_parser.events:
        eid = event['id']
        if eid in event_defs:
            msg_tmpl = event_defs[eid]['message']
            msg = format_msg(msg_tmpl, event['extra'],
                             indent=2*conf['report_indent'],
                             width=conf['report_width'])
            rem_tmpl = event_defs[eid]['remedy']
            rem = format_msg(rem_tmpl, event['extra'],
                             indent=2*conf['report_indent'],
                             width=conf['report_width'])
            if event_defs[eid]['category'] == 'error':
                cat = 'E'
                nr_errors += 1
            elif event_defs[eid]['category'] == 'warning':
                cat = 'W'
                nr_warnings += 1
            if 'line' in event and event['line']:
                output_fmt = ('{cat} syntax on line {line:d}:\n'
                              '{indent}problem:\n'
                              '{msg}\n'
                              '{indent}remedy:\n'
                              '{rem}')
            else:
                output_fmt = ('{cat} semantics:\n'
                              '{indent}problem:\n'
                              '{msg}\n'
                              '{indent}remedy:\n'
                              '{rem}')
            print output_fmt.format(cat=cat, line=event['line'],
                                    msg=msg, rem=rem, indent=indent)
        else:
            msg = "### internal error: unknown event id '{0}'\n"
            sys.stderr.write(msg.format(id))
            sys.exit(UNDEFINED_EVENT)
    if not options.quiet:
        print '{err:d} errors, {warn:d} warnings'.format(warn=nr_warnings,
                                                         err=nr_errors)
    if options.show_job:
        print pbs_parser.job.attrs_to_str()
    if nr_errors > 0:
        sys.exit(ERRORS_EXIT)
    elif options.warnings_as_errors and nr_warnings > 0:
        sys.exit(WARNINGS_EXIT)
    else:
        sys.exit(NO_ERRORS_EXIT)


#!/usr/bin/env python

NO_ERRORS_EXIT = 0
ERRORS_EXIT = 1
WARNINGS_EXIT = 2
CAN_NOT_OPEN_EVENT_FILE = 11
CAN_NOT_OPEN_CONF_FILE = 12
CAN_NOT_OPEN_CLUSTER_DB_FILE = 13
CAN_NOT_OPEN_PBS = 14
UNDEFINED_EVENT = 15

if __name__ == '__main__':
    from argparse import ArgumentParser
    import json, os, sqlite3, sys
    from vsc.pbs.script_parser import PbsScriptParser
    from vsc.pbs.check import JobChecker

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
    nr_warnings = 0
    nr_errors = 0
    for event in pbs_parser.events:
        eid = event['id']
        if eid in event_defs:
            msg_tmpl = event_defs[eid]['message']
            msg = msg_tmpl.format(**event['extra'])
            rem_tmpl = event_defs[eid]['remedy']
            rem = rem_tmpl.format(**event['extra'])
            if event_defs[eid]['category'] == 'error':
                cat = 'E'
                nr_errors += 1
            elif event_defs[eid]['category'] == 'warning':
                cat = 'W'
                nr_warnings += 1
            if 'line' in event and event['line']:
                output_fmt = ('{cat} syntax on line {line:d}:\n'
                              '    problem: {msg}\n'
                              '    remedy:  {rem}')
            else:
                output_fmt = ('{cat} semantics:\n'
                              '    problem: {msg}\n'
                              '    remedy:  {rem}')
            print output_fmt.format(cat=cat, line=event['line'],
                                    msg=msg, rem=rem)
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


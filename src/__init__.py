#!/usr/bin/python
# vim: noet sw=4 ts=4

import	sys
import	os
import	subprocess
from	multiprocessing		import	Process,Pool,cpu_count
from	optparse			import	OptionParser
import	datetime
import	time
from	version				import	version
import	signal
import	atexit

FAIL    = '- '					# Set this, and ...
SUCCESS = ' ' * len(FAIL)		# this will follow

groups       = None
interval     = 15				# In minutes
inventory    = dict()
inventory_fn = 'inventory'		# Where we get detectives
me           = 'oswatcher'
opts         = dict( jitter = 0.0 )
overrun      = False
pool         = None
prefix       = 'Archive'		# Top-level directory for files
retention    = 48				# In hours
umask        = 0660				# Umask for files, directories are 0775

def	sigusr1_handler( signum, frame ):
	global	opts
	print 'frame is a {0}'.format( type( frame ) )
	print 'opts is a {0}'.format( type( opts ) )
	for key in sorted( opts ):
		print '{0} = {0}'.format( key, opts[key] )
	return

def	sigusr1_setup():
	signal.signal( signal.SIGUSR1, sigusr1_handler )
	return

def	watchdog_expiry( signum, frame ):
	overrun = True
	raise IOError( 'Watchdog expiry' )

def	watchdog_setup( callback = watchdog_expiry ):
	signal.signal( signal.SIGALRM, callback )
	return

def	watchdog_start( period = (interval * 60 * 2) - 1 ):
	signal.alarm( period )
	return

def	watchdog_stop():
	signal.alarm( 0 )
	return

def	timestamp( local = False ):
	getter = datetime.datetime.fromtimestamp if local else datetime.datetime.utcfromtimestamp
	ts = getter( time.time() )
	return ts.strftime(
		r'%Y-%m-%d-%H:%M'
	)

def	show_inventory( inventory ):
	FMT = '  {0:>15.15}  {1}'
	bars = '-' * (80-15-2-2)
	print FMT.format( 'Group', 'Commands' )
	print FMT.format(
		bars,
		bars
	)
	for group in sorted( inventory ):
		print '  {0:>15}  {1}'.format(
			group,
			inventory[group]
		)
	return

def	heartbeat( ts, notice, f = sys.stdout, others = False ):
	print >>f, '{0}zzz ***{1} ({2})\n'.format(
		'\n' if others else '',
		ts,
		notice
	)
	return

def	worker( (group, ts) ):
	global	opts
	# Must have a logging directory.
	destdir = os.path.join(
		opts.prefix,
		group
	)
	if not os.path.isdir( destdir ):
		try:
			os.makedirs( destdir, 0775 )
		except OSError:
			pass
	#
	logs = [
		f for f in os.listdir( destdir ) if os.path.isfile( f )
	]
	for goner in logs[:-retention]:
		history = os.path.join(
			destdir,
			goner
		)
		try:
			os.unlink( history )
			print >>sys.stderr, '--- {0}'.format( history )
		except KeyboardInterrupt, e:
			raise e('Pruning archive history')
		except Exception, e:
			pass
	#
	# Timestamp is to the minute, but the filename is to the hour
	#
	fn = os.path.join(
		destdir,
		'{0}-{1}-{2}.dat'.format(
			os.uname()[1],
			ts[:-3],
			group,
		)
	)
	others = False
	with open( fn, 'a' ) as f:
		cmd = inventory[group]
		heartbeat( ts, cmd, f = f, others = others )
		try:
			p = subprocess.Popen(
				cmd,
				bufsize = 8192,
				stdin   = None,
				stdout  = subprocess.PIPE,
				stderr  = subprocess.PIPE,
				shell   = True
			)
			out, err = p.communicate( input = None )
			if out:
				print >>f, out
			if err:
				print >>f
				s = 'Error Messages'
				print >>f, s
				print >>f, '-' * len( s )
				print >>f
				print >>f, err
		except KeyboardInterrupt, e:
			raise e('performing {0} step'.format( group ) )
		except Exception, e:
			print >>f, '\n*** Something went wrong\n'
			print >>f, e
		others = True
	return group,err

def	do_one_event_loop():
	global	inventory
	ts = timestamp()
	print >>sys.stderr, ts
	results = pool.imap_unordered(
		worker,
		[ (group,ts) for group in inventory ],
		8192
	)
	for group,err in sorted( results ):
		if err and len(err) > 0:
			print >>sys.stderr, '*** {0}'.format( group )
			print >>sys.stderr, '    {0}'.format( inventory[group] )
			for line in err.splitlines():
				print >>sys.stderr, '    {0}'.format( line )
	return

def	event_loop():
	"""
		Each iteration of this loop collects 1 sampling of all the
		enabled measurement groups.  We try hard to initiate the
		samples at a constant interval, and not introduce any
		time skew just because one sampling ran too long.  If it
		does, we just have a shorter wait and still start the next
		collection activity on time.  To help with this, we keep
		a running standard deviation of the interval deltas, so a
		standard deviation of 1.3 means we are off about 1.3 seconds
		from the ideal.
	"""
	import	jitter
	global	opts
	jitter = jitter.Jitter()
	# Fake first sample as perfect value
	jitter.add_sample( 0.0 )
	tick   = datetime.timedelta( minutes = opts.interval )
	margin = datetime.timedelta(
		minutes = opts.interval * 0.10
	).total_seconds()
	clock  = datetime.datetime.now() + datetime.timedelta( seconds = 5 )
	while True:
		# Make sure we didn't get here too late
		current = datetime.datetime.now()
		delta   = (clock - current).total_seconds()
		if delta < 0.0 and abs(delta) > margin:
			# We came back here late
			print '*** Overrun by {0} seconds'.format( -delta )
		jitter.add_sample( delta )
		clock   += tick
		# Take a sample
		watchdog_start()
		quit = do_one_event_loop()
		watchdog_stop()
		if quit:
			print '*** Cancelled by request'
			break
		# Sleep until next activity period
		current  = datetime.datetime.now()
		delta    = (clock - current).total_seconds()
		nap      = max( 0.0, delta )
		if nap > 0.0:
			time.sleep( nap )
	return

def	cleanup( pid_file ):
	try:
		os.unlink( pid_file )
	except:
		pass
	return

def	main():
	#
	global	retention
	global	prefix
	global	interval
	global	inventory
	global	pool
	#
	retention      = opts.retention
	prefix         = opts.prefix
	interval       = opts.interval
	#
	inventory = dict()
	with open( inventory_fn ) as f:
		for line in f:
			tokens = map(
				str.strip,
				line.split('#',1)[0].split(None,1)
			)
			if len(tokens) == 2:
				group = tokens[0]
				action = '; '.join( tokens[1:] )
				inventory[group] = action
				print 'inventory[{0}] = {1}'.format( group, action )
	if not opts.groups:
		opts.groups = inventory.keys()
	for group in inventory:
		if group not in opts.groups:
			inventory.pop(group)
	show_inventory( inventory )
	print 'opts={0}'.format( opts )
	print 'args={0}'.format( args )
	#
	watchdog_setup( callback = watchdog_expiry )
	sigusr1_setup()
	#
	pool = Pool( processes = opts.worker_count )
	event_loop()
	return

if __name__ == '__main__':
	me = os.path.splitext(
		os.path.basename(
			os.path.expanduser( sys.argv[0] )
		)
	)[0]
	inventory = dict()
	p = OptionParser(
		prog = me,
		version = version.VERSION,
		usage = '{0} [options] <tarball> ...'.format(
			os.path.basename(
				os.path.splitext(sys.argv[0])[0]
			)
		)
	)
	p.add_option(
		'-p',
		'--prefix',
		metavar = 'dir',
		dest    = 'prefix',
		default = prefix,
		help    = 'top level of results; default is "{0}{1}"'.format(
			prefix,
			os.path.sep
		)
	)
	p.add_option(
		'-k',
		'--keep',
		metavar = 'N',
		dest    = 'retention',
		type    = int,
		default = retention,
		help    = 'hours of results to keep; default is {0} hours'.format(
			retention
		)
	)
	p.add_option(
		'-i',
		'--interval',
		dest    = 'interval',
		metavar = 'N',
		type    = int,
		default = interval,
		help    = 'sample exery N minutes; default is {0} minutes'.format(
			interval
		)
	)
	p.add_option(
		'-d',
		'--do',
		metavar = 'list',
		dest = 'groups',
		default = None,
		help = 'comma list of groups to run; defaults to all groups'
	)
	worker_count = max( cpu_count() - 2, 1 )
	p.add_option(
		'-w',
		'--workers',
		metavar = 'N',
		dest = 'worker_count',
		type = int,
		default = worker_count,
		help = 'worker threads; default = {0}'.format( worker_count )
	)
	pid_file = '/run/oswatcher.pid'
	p.add_option(
		'-r',
		'--run',
		metavar = 'path',
		dest = 'pid_file',
		type = str,
		default = pid_file,
		help = 'write PID here; default = {0}'.format( pid_file )
	)
	( opts, args ) = p.parse_args()
	if os.geteuid():
		print >>sys.stderr, 'Must be run by root'
		exit(1)
	atexit.register( cleanup, opts.pid_file )
	with open( opts.pid_file, 'w' ) as f:
		print >>f, os.getpid()
	main()
	exit( 0 )

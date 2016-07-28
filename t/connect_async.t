use Test::More;

plan skip_all => 'set TEST_ONLINE to enable this test'
  unless $ENV{TEST_ONLINE};

use Rethinkdb;
use Data::Dumper;

# initialization
my $r = Rethinkdb->new;
isa_ok $r, 'Rethinkdb';

$r = r;
isa_ok $r, 'Rethinkdb';

my $conn = r->connect;
isa_ok $conn, 'Rethinkdb::IO::Async';

# connect default values
is $conn->host,       'localhost';
is $conn->port,       28015;
is $conn->default_db, 'test';
is $conn->auth_key,   '';
is $conn->timeout,    20;

# other values for connect
if (eval { my $r = r->connect('wiggle', undef, undef, undef, undef, {blocking => 1}); }) {
    fail ('connect did not fail');
} else {
  like $@, qr/ERROR: Could not connect to wiggle:28015/,
    'Correct host connection error message';
}

if (eval { my $r = r->connect('localhost', 48015, undef, undef, undef, {blocking => 1}); } ) {
    fail ('connect did not fail');
} else {
  like $@, qr/ERROR: Could not connect to localhost:48015/,
    'Correct host connection error message';
}

$r = r->connect( 'localhost', 28015, 'better', undef, undef, {blocking => 1} );
isnt $r->default_db, 'test';
is $r->default_db, 'better', 'Correct `default_db` set';

# test auth_key
eval { r->connect( 'localhost', 28015, 'better', 'hiddenkey' ); } or do {
  like $@, qr/ERROR: Incorrect authorization key./,
    'Correct `auth_key` connection error message';
};

my $r = r->connect( 'localhost', 28015, 'better', '', 100 );
is $r->timeout, 100, 'Correct timeout set';

# query without connection should throw an error
if (eval { r->db('test')->create->run; }) {
    fail ('run did not die');
}else {
  like $@, qr/ERROR: run\(\) was not given a connection/,
    'Correct error on `run` without connection';
};

# internal stuff
r->connect;
is r->io, undef;

r->connect->repl;
isa_ok r->io, 'Rethinkdb::IO::Async';

# close connection
$conn = r->connect;
isa_ok $conn->close, 'Rethinkdb::IO::Async';
is $conn->_handle,   undef;

$conn = r->connect;
isa_ok $conn->close( noreply_wait => 0 ), 'Rethinkdb::IO::Async';
is $conn->_handle, undef;

# reconnect
isa_ok $conn->reconnect, 'Rethinkdb::IO::Async';
isa_ok $conn->_handle,   'AnyEvent::Handle';

isa_ok $conn->reconnect( noreply_wait => 0 ), 'Rethinkdb::IO::Async';
isa_ok $conn->_handle, 'AnyEvent::Handle';

# switch default databases
$conn->use('test2');
is $conn->default_db, 'test2';

$conn->use('wiggle-waggle');
is $conn->default_db, 'wiggle-waggle';

# noreply_wait
my $cv = AnyEvent->condvar;
$conn->noreply_wait(sub {$cv->send(shift)});
my $res = $cv->recv;
is $res->type_description, 'wait_complete';

# testing run parameters

# profile
my $cv = AnyEvent->condvar;
r->db('rethinkdb')->table('logs')->nth(0)->run( { profile => r->true }, sub {$cv->send(shift)} );
my $res = $cv->recv;
ok defined($res), 'respsponse is defined';
diag Dumper($res);
isa_ok $res->profile, 'ARRAY', 'Correctly received profile data';

my $cv = AnyEvent->condvar;
# durability (no real way to test the output)
r->db('test')->drop->run(sub {$cv->send(shift)});
$cv->recv;
my $cv = AnyEvent->condvar;
r->db('test')->create->run( { durability => 'soft' }, sub {$cv->send(shift)} );
$cv->recv;
my $cv = AnyEvent->condvar;

r->db('test')->table('battle')->create->run(sub {$cv->send(shift)});
$cv->recv;
my $cv = AnyEvent->condvar;
r->db('test')->table('battle')->insert(
  [
    {
      id           => 1,
      superhero    => 'Iron Man',
      target       => 'Mandarin',
      damage_dealt => 100,
    },
    {
      id           => 2,
      superhero    => 'Wolverine',
      target       => 'Sabretooth',
      damage_dealt => 40,
    },
    {
      id           => 3,
      superhero    => 'Iron Man',
      target       => 'Magneto',
      damage_dealt => 90,
    },
    {
      id           => 4,
      superhero    => 'Wolverine',
      target       => 'Magneto',
      damage_dealt => 10,
    },
    {
      id           => 5,
      superhero    => 'Spider-Man',
      target       => 'Green Goblin',
      damage_dealt => 20,
    }
  ]
)->run(sub {$cv->send(shift)});
$cv->recv;

# group_format
my $cv = AnyEvent->condvar;
r->db('test')->table('battle')->group('superhero')
    ->run( { group_format => 'raw' }, sub {$cv->send(shift)} );
$res = $cv->recv();

diag Dumper($res);
is $res->response->{'$reql_type$'}, 'GROUPED_DATA',
  'Correct group_format response data';
isa_ok $res->response->{data}, 'ARRAY', 'Correct group_format response data';
isa_ok $res->response->{data}[0][1], 'ARRAY',
  'Correct group_format response data';

# db
my $cv = AnyEvent->condvar;
r->table('cluster_config')->run( { db => 'rethinkdb' }, sub {$cv->send(shift)} );
$res = $cv->recv;
ok(
       $res->response->[0]->{id} eq 'auth'
    or $res->response->[0]->{id} eq 'heartbeat'
  ),
  'Correct response for db change';

# array_limit (doesn't seem to change response)
r->db('test')->table('battle')->run( { array_limit => 2 } );

# noreply
#$res = r->db('test')->table('battle')->run( { noreply => 1 } );
#is $res, undef, 'Correct response for noreply';

# test a callback
my $cv = AnyEvent->condvar;
$res = r->db('test')->table('battle')->run(
  sub {
      my $res = shift;
      $cv->send($res);
  }
 );
my $res = $cv->recv;
isa_ok $res, 'Rethinkdb::Response', 'Correct response for callback';

# check default database parameter is being used
r->connect( 'localhost', 28015, 'random' . int( rand(1000) ) )->repl;
my $cv = AnyEvent->condvar;
r->table('superheroes')->create->run(
    sub {
        my $res = shift;
        my $partial = shift;
        $cv->send($res) unless $partial;
    }
);
my $res = $cv->recv;
diag Dumper($res);

is $res->{error_type}, 4100000, 'Expected error_type';
is ref($res->{response}), 'ARRAY', "Response is an array";

# failes
#like $res->{response}->[0], qr/Database `random[0-9]+` does not exist./;

# server information
my $cv = AnyEvent->condvar;
r->server(sub {
              $cv->send(shift)
          });
my $res = $cv->recv();

is $res->type, 5, 'Expected response type';
is_deeply [ sort( keys %{ $res->response->[0] } ) ],
  [ 'id', 'name', 'proxy' ], 'Correct response keys';
like $res->response->[0]->{id},
  qr/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/,
  'Correct response';

my $cv = AnyEvent->condvar;
$conn = r->connect;
$conn->server(sub {$cv->send(shift)});
my $res = $cv->recv;

is $res->type, 5, 'Expected response type';
is_deeply [ sort( keys %{ $res->response->[0] } ) ],
  [ 'id', 'name', 'proxy' ], 'Correct response keys';
like $res->response->[0]->{id},
  qr/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/,
  'Correct response';

# clean up
r->db('test')->drop->run;

done_testing();

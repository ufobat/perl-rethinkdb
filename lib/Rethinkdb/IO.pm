package Rethinkdb::IO;
use Rethinkdb::Base -base;

no warnings 'recursion';

use Carp 'croak';
use JSON::PP;

use Rethinkdb::IO::Async;
use Rethinkdb::Protocol;
use Rethinkdb::Response;

has host       => 'localhost';
has port       => 28_015;
has default_db => 'test';
has auth_key   => q{};
has timeout    => 20;
has args       => sub { {} };
has [ '_rdb', '_handle', '_callbacks', '_responder' ];
has '_protocol' => sub { Rethinkdb::Protocol->new; };

sub new {
     my $class = shift;
     if (ref($class) eq 'Rethinkdb::IO' or $class eq 'Rethinkdb::IO') {
         return Rethinkdb::IO::Async->new(@_);
     }elsif (ref($class) =~ m"^Rethinkdb::IO" or $class =~ m"^Rethinkdb::IO") {
         return $class->SUPER::new(@_);
     } else {
         croak('ERROR: Please use Rethinkdb::IO::Async or Rethinkdb::IO::Sync');
     }
}

sub close {
  my $self = shift;
  my $args = ref $_[0] ? $_[0] : {@_};

  if ( $self->_handle ) {
    if ( !defined $args->{noreply_wait} || !$args->{noreply_wait} ) {
      $self->noreply_wait;
    }

    $self->_handle->destroy if $self->_handle->can('destroy'); # async
    $self->_handle->close if $self->_handle->can('close'); # sync
    $self->_handle(undef);
  }

  $self->_callbacks( {} );

  return $self;
}

sub reconnect {
  my $self = shift;
  my $args = ref $_[0] ? $_[0] : {@_};

  return $self->close($args)->connect;
}

# put the handle into main package
sub repl {
  my $self    = shift;
  my $package = caller || 'main';

  no strict 'refs';
  ${$package . '::_rdb_io'} = $self;
  return $self;
}

sub use {
  my $self = shift;
  my $db   = shift;

  $self->default_db($db);
  return $self;
}

sub noreply_wait {
    my $self = shift;
    my $callback = shift;

    my $token = Rethinkdb::Util::_token();
    if ( ref $callback eq 'CODE' ) {
        $self->_callbacks->{ $token } = $callback;
    }

    return $self->_send(
        {
            type  => $self->_protocol->query->queryType->noreply_wait,
            token => $token
        },
    );
}

sub server {
  my $self = shift;
  my $callback = shift;

  my $token = Rethinkdb::Util::_token();
  if ( ref $callback eq 'CODE' ) {
      $self->_callbacks->{ $token } = $callback;
  }

  return $self->_send(
    {
      type  => $self->_protocol->query->queryType->server_info,
      token => $token
    },
  );
}

sub _start {
  my $self = shift;
  my ( $query, $args, $callback ) = @_;

  my $q = {
    type  => $self->_protocol->query->queryType->start,
    token => Rethinkdb::Util::_token(),
    query => $query->_build
  };

  if ( ref $callback eq 'CODE' ) {
    $self->_callbacks->{ $q->{token} } = $callback;
  }

  # add our database
  if ( !$args->{db} ) {
    $args->{db} = $self->default_db;
  }

  return $self->_send( $q, $args );
}

sub _encode {
  my $self = shift;
  my $data = shift;
  my $args = shift || {};

  # only QUERY->START needs these:
  if ( $data->{type} == 1 ) {
    $data = $self->_encode_recurse($data);
    push @{$data}, _simple_encode_hash($args);
  }
  else {
    $data = [ $data->{type} ];
  }

  return encode_json $data;
}

# temporarily: clean up global optional arguments
sub _simple_encode_hash {
  my $data = shift;
  my $json = {};

  foreach ( keys %{$data} ) {
    $json->{$_} = _simple_encode( $data->{$_} );
  }

  if ( $json->{db} ) {
    $json->{db} = Rethinkdb::IO->_encode_recurse(
      Rethinkdb::Query::Database->new(
        name => $json->{db},
        args => $json->{db},
      )->_build
    );
  }

  return $json;
}

sub _simple_encode {
  my $data = shift;

  if ( ref $data eq 'Rethinkdb::_True' ) {
    return JSON::PP::true;
  }
  elsif ( ref $data eq 'Rethinkdb::_False' ) {
    return JSON::PP::false;
  }

  return $data;
}

sub _encode_recurse {
  my $self = shift;
  my $data = shift;
  my $json = [];

  if ( $data->{datum} ) {
    my $val = q{};
    if ( defined $data->{datum}->{r_bool} ) {
      if ( $data->{datum}->{r_bool} ) {
        return JSON::PP::true;
      }
      else {
        return JSON::PP::false;
      }
    }
    elsif ( defined $data->{datum}->{type}
      && $data->{datum}->{type} == $self->_protocol->datum->datumType->r_null )
    {
      return JSON::PP::null;
    }
    else {
      foreach ( keys %{ $data->{datum} } ) {
        if ( $_ ne 'type' ) {
          return $data->{datum}->{$_};
        }
      }
    }
  }

  if ( $data->{type} ) {
    push @{$json}, $data->{type};
  }

  if ( $data->{query} ) {
    push @{$json}, $self->_encode_recurse( $data->{query} );
  }

  if ( $data->{args} ) {
    my $args = [];
    foreach ( @{ $data->{args} } ) {
      push @{$args}, $self->_encode_recurse($_);
    }

    push @{$json}, $args;
  }

  if ( $data->{optargs} && ref $data->{optargs} eq 'HASH' ) {
    push @{$json}, $self->_encode_recurse( $data->{optargs} );
  }
  elsif ( $data->{optargs} ) {
    my $args = {};
    foreach ( @{ $data->{optargs} } ) {
      $args->{ $_->{key} } = $self->_encode_recurse( $_->{val} );
    }

    if ( $data->{type} == $self->_protocol->term->termType->make_obj ) {
      return $args;
    }

    push @{$json}, $args;
  }

  return $json;
}

sub _decode {
  my $self   = shift;
  my $data   = shift;
  my $decode = decode_json $data;

  $decode->{r} = $self->_clean( $decode->{r} );
  return $decode;
}

# converts JSON::PP::Boolean in an array to our Booleans
sub _clean {
  my $self  = shift;
  my $data  = shift;
  my $clean = [];

  if ( ref $data eq 'ARRAY' ) {
    foreach ( @{$data} ) {
      push @{$clean}, $self->_real_cleaner($_);
    }

    return $clean;
  }
  elsif ( ref $data eq 'HASH' ) {
    foreach ( keys %{$data} ) {
      $data->{$_} = $self->_real_cleaner( $data->{$_} );
    }

    return $data;
  }

  return $data;
}

sub _real_cleaner {
  my $self = shift;
  my $data = shift;
  my $retval;

  if ( ref $data eq 'JSON::PP::Boolean' ) {
    if ($data) {
      $retval = $self->_rdb->true;
    }
    else {
      $retval = $self->_rdb->false;
    }
  }
  elsif ( ref $data eq 'ARRAY' ) {
    $retval = $self->_clean($data);
  }
  elsif ( ref $data eq 'HASH' ) {
    $retval = $self->_clean($data);
  }
  else {
    $retval = $data;
  }

  return $retval;
}

1;

=encoding utf8

=head1 NAME

Rethinkdb::IO - RethinkDB IO

=head1 SYNOPSIS

  package MyApp;
  use Rethinkdb::IO;

  my $io = Rethinkdb::IO->new->connect;
  $io->use('marvel');
  $io->close;

=head1 DESCRIPTION

This module handles communicating with the RethinkDB Database.

=head1 ATTRIBUTES

L<Rethinkdb::IO> implements the following attributes.

=head2 host

  my $io = Rethinkdb::IO->new->connect;
  my $host = $io->host;
  $io->host('r.example.com');

The C<host> attribute returns or sets the current host name that
L<Rethinkdb::IO> is currently set to use.

=head2 port

  my $io = Rethinkdb::IO->new->connect;
  my $port = $io->port;
  $io->port(1212);

The C<port> attribute returns or sets the current port number that
L<Rethinkdb::IO> is currently set to use.

=head2 default_db

  my $io = Rethinkdb::IO->new->connect;
  my $port = $io->default_db;
  $io->default_db('marvel');

The C<default_db> attribute returns or sets the current database name that
L<Rethinkdb::IO> is currently set to use.

=head2 auth_key

  my $io = Rethinkdb::IO->new->connect;
  my $port = $io->auth_key;
  $io->auth_key('setec astronomy');

The C<auth_key> attribute returns or sets the current authentication key that
L<Rethinkdb::IO> is currently set to use.

=head2 timeout

  my $io = Rethinkdb::IO->new->connect;
  my $timeout = $io->timeout;
  $io->timeout(60);

The C<timeout> attribute returns or sets the timeout length that
L<Rethinkdb::IO> is currently set to use.

=head1 METHODS

L<Rethinkdb::IO> inherits all methods from L<Rethinkdb::Base> and implements
the following methods.

=head2 connect

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect->repl;

The C<connect> method initiates the connection to the RethinkDB database.

=head2 close

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect;
  $io->close;

The C<connect> method closes the current connection to the RethinkDB database.

=head2 reconnect

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect;
  $io->reconnect;

The C<reconnect> method closes and reopens a connection to the RethinkDB
database.

=head2 repl

  my $io = Rethinkdb::IO->new;
  $io->host('rdb.example.com');
  $io->connect->repl;

The C<repl> method caches the current connection in to the main program so that
it is available to for all L<Rethinkdb> queries without specifically specifying
one.

=head2 use

  my $io = Rethinkdb::IO->new;
  $io->use('marven');
  $io->connect;

The C<use> method sets the default database name to use for all queries that
use this connection.

=head2 noreply_wait

  my $io = Rethinkdb::IO->new;
  $io->noreply_wait;

The C<noreply_wait> method will tell the database to wait until all "no reply"
have executed before responding.

=head2 server

  my $conn = r->connect;
  $conn->server;

Return information about the server being used by this connection.

The server command returns either two or three fields:

=over

=item C<id>: the UUID of the server the client is connected to.

=item C<proxy>: a boolean indicating whether the server is a L<RethinkDB proxy node|http://rethinkdb.com/docs/sharding-and-replication/#running-a-proxy-node>.

=item C<name>: the server name. If proxy is C<< r->true >>, this field will not
be returned.

=back

=head1 SEE ALSO

L<Rethinkdb>, L<http://rethinkdb.com>

=cut

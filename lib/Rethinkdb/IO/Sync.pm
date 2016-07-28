package Rethinkdb::IO::Sync;
use Rethinkdb::Base 'Rethinkdb::IO';

no warnings 'recursion';

use Carp 'croak';
use IO::Socket::INET;
use JSON::PP;

use Rethinkdb::Response;

sub connect {
  my $self = shift;

  $self->{_handle} = IO::Socket::INET->new(
    PeerHost => $self->host,
    PeerPort => $self->port,
    Reuse    => 1,
    Timeout  => $self->timeout,
    )
    or croak 'ERROR: Could not connect to ' . $self->host . q{:} . $self->port;

  $self->_handle->send( pack 'L<',
    $self->_protocol->versionDummy->version->v0_3 );
  $self->_handle->send(
    ( pack 'L<', length $self->auth_key ) . $self->auth_key );

  $self->_handle->send( pack 'L<',
    $self->_protocol->versionDummy->protocol->json );

  my $response;
  my $char = q{};
  do {
    $self->_handle->recv( $char, 1 );
    $response .= $char;
  } while ( $char ne "\0" );

  # trim string
  $response =~ s/^\s//;
  $response =~ s/\s$//;

  if ( $response =~ /^ERROR/ ) {
    croak $response;
  }

  $self->_callbacks( {} );

  return $self;
}

sub _send {
  my $self  = shift;
  my $query = shift;
  my $args  = shift || {};

  if ( $ENV{RDB_DEBUG} ) {
    use feature ':5.10';
    use Data::Dumper;
    $Data::Dumper::Indent = 1;
    say {*STDERR} 'SENDING:';
    say {*STDERR} Dumper $query;
  }

  my $token;
  my $length;

  my $serial = $self->_encode( $query, $args );
  my $header = pack 'QL<', $query->{token}, length $serial;

  if ( $ENV{RDB_DEBUG} ) {
    say 'SENDING:';
    say {*STDERR} Dumper $serial;
  }

  # send message
  $self->_handle->send( $header . $serial );

  # noreply should just return
  if ( $args->{noreply} ) {
    return;
  }

  # receive message
  my $data = q{};

  $self->_handle->recv( $token, 8 );
  $token = unpack 'Q<', $token;

  $self->_handle->recv( $length, 4 );
  $length = unpack 'L<', $length;

  # if we couldn't unpack a length, say it is zero
  $length ||= 0;

  my $_data;
  do {
    $self->_handle->recv( $_data, 4096 );
    $data = $data . $_data;
  } until ( length($data) eq $length );

  # decode RQL data
  my $res_data = $self->_decode($data);
  $res_data->{token} = $token;

  # handle partial response
  if ( $res_data->{t} == 3 ) {
    if ( $self->_callbacks->{$token} ) {
      my $res = Rethinkdb::Response->_init( $res_data, $args );

      if ( $ENV{RDB_DEBUG} ) {
        say {*STDERR} 'RECEIVED:';
        say {*STDERR} Dumper $res;
      }

      # send what we have
      $self->_callbacks->{$token}->($res);

      # fetch more
      return $self->_send(
        {
          type  => $self->_protocol->query->queryType->continue,
          token => $token
        }
      );
    }
    else {
      if ( $ENV{RDB_DEBUG} ) {
        say {*STDERR} 'RECEIVED:';
        say {*STDERR} Dumper $res_data;
      }

      # fetch the rest of the data if partial
      my $more = $self->_send(
        {
          type  => $self->_protocol->query->queryType->continue,
          token => $token
        }
      );

      push @{ $res_data->{r} }, @{ $more->response };
      $res_data->{t} = $more->type;
    }
  }

  # put data in response
  my $res = Rethinkdb::Response->_init( $res_data, $args );

  if ( $ENV{RDB_DEBUG} ) {
    say {*STDERR} 'RECEIVED:';
    say {*STDERR} Dumper $res_data;
    say {*STDERR} Dumper $res;
  }

  # if there is callback return data to that
  if ( $self->_callbacks->{$token} ) {
    my $cb = $self->_callbacks->{$token};
    delete $self->_callbacks->{$token};
    return $cb->($res);
  }

  return $res;
}

1;

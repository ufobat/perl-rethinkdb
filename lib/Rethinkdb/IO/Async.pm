package Rethinkdb::IO::Async;
use Rethinkdb::Base 'Rethinkdb::IO';

no warnings 'recursion';

use Carp 'croak';
use AnyEvent::Handle;
use JSON::PP;

use Rethinkdb::Response;

sub connect {
    my $self = shift;

    my $condvar;
    if ($self->args->{blocking}) {
        $condvar = AnyEvent->condvar;
    }

    $self->_handle(
        AnyEvent::Handle->new(
            connect          => [ $self->host, $self->port ],
            on_connect_error => sub {
                my ($hdl, $msg) = @_;
                my $error = 'ERROR: Could not connect to '
                    . $self->host . q{:}
                    . $self->port . " $msg";
                _cv_error($condvar, $error);
            },
            on_connect => sub {
                $condvar->send("SUCCESS") if ($condvar);
            },
            on_error => sub {
                my ($hdl, $fatal, $msg) = @_;
                if ($fatal) {
                    _cv_error($condvar, "ERROR: $msg");
                    delete $self->{_handle};
                }
                else {
                    _cv_error($condvar, "WARN: $msg");
                }
            },
        )
    );

    $self->_handle->push_write(pack 'L<',
        $self->_protocol->versionDummy->version->v0_3);
    $self->_handle->push_write(
        (pack 'L<', length $self->auth_key) . $self->auth_key);

    $self->_handle->push_write(pack 'L<',
        $self->_protocol->versionDummy->protocol->json);

    $self->_handle->push_read(
        line => "\0",
        sub {
            my ($hdl, $response) =@_;
            $response =~ s/^\s//;
            $response =~ s/\s$//;

            if ( $response =~ /^ERROR/ ) {
                _cv_error($condvar, $response);
            }
        }
    );
    $self->_callbacks( {} );

    if ($condvar) {
        my $msg = $condvar->recv();
        unless ($msg eq "SUCCESS") {
            croak $msg;
        }
    }

    return $self;
}

sub _cv_error {
    my ($cv, $error) = @_;
    if ($cv) {
        $cv->send($error);
    } else {
        croak $error;
    }
}

sub _send {
  my $self  = shift;
  my $query = shift;
  my $args  = shift || {};
  my $res   = shift;

  my %args = (%$args, %{$self->args});
  $args = \%args;

  if ( $ENV{RDB_DEBUG} ) {
    use feature ':5.10';
    use Data::Dumper;
    $Data::Dumper::Indent = 1;
    say {*STDERR} 'SENDING:';
    say {*STDERR} Dumper $query;
  }


  my $serial = $self->_encode( $query, $args );
  my $header = pack 'QL<', $query->{token}, length $serial;

  if ( $ENV{RDB_DEBUG} ) {
    say 'SENDING:';
    say {*STDERR} Dumper $serial;
  }

  # send message
  $self->_handle->push_write( $header . $serial );

  # noreply should just return
  # doesnt that mean that we lose sync with the connection?
  #if ( $args->{noreply} ) {
  #  return;
  #}

  my $condvar;
  if ($args->{blocking}) {
      $condvar = AnyEvent->condvar;
  }

  $self->_handle->push_read(
      chunk => 12,
      sub {
          my ($hdl, $data) = @_;
          my ($token, $length) = unpack('QL<', $data);
          # say STDERR localtime(time) . " reading $length of data token=$token";
          $hdl->unshift_read(
              chunk => $length,
              sub {
                  my ($hdl, $data) = @_;
                  if (exists $self->_callbacks->{$token}) {

                      # decode RQL data
                      my $res_data = $self->_decode($data);
                      $res_data->{token} = $token;

                      if ( $ENV{RDB_DEBUG} ) {
                          say {*STDERR} 'RECEIVED:';
                          say {*STDERR} Dumper $res;
                      }

                      if ($res_data->{t} == 3) {
                          if ($res) {
                              $res->_append($res_data, $args);
                          } else {
                              $res = Rethinkdb::Response->_init( $res_data, $args );
                          }

                          $self->_send(
                              {
                                  type  => $self->_protocol->query->queryType->continue,
                                  token => $token
                              },
                              $args,
                              $res
                          );
                          unless ($args->{no_partial_updates}) {
                              eval {
                                  $self->_callbacks->{$token}->($res, 'incomplete');
                              };
                          }
                      } else {
                          $res = Rethinkdb::Response->_init( $res_data, $args );
                          eval {
                              $self->_callbacks->{$token}->($res);
                          };
                          $condvar->send() if $condvar;
                      }
                  }
              }
          );
      }
     );

    $condvar->recv if $condvar;
}

1;


=head1 LICENSE

Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
Copyright [2016-2019] EMBL-European Bioinformatics Institute

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

=cut

package Bio::EnsEMBL::DBLoader::RunnableDB::Database;

use strict;
use warnings;

use Bio::EnsEMBL::DBSQL::DBConnection;
use File::Temp qw(tempfile);

sub sql_helper {
  my ($self) = @_;
  return $self->target_dbc()->sql_helper();
}

sub target_dbc {
  my ($self) = @_;
  return $self->param('target_dbc')
    if $self->param_is_defined('target_dbc');
  my $details = $self->param('target_db');
  my $dbc     = Bio::EnsEMBL::DBSQL::DBConnection->new( %{$details},
                                              -reconnect_if_lost => 1 );
  return $self->param( 'target_dbc', $dbc );
}

sub switch_db {
  my ( $self, $db ) = @_;
  $self->target_dbc()->disconnect_if_idle();
  $self->target_dbc()->dbname($db);
  return;
}

sub database {
  my ($self) = @_;
  return $self->param('database');
}

sub db_hash {
  my ($self) = @_;
  return {
       map { $_ => 1 }
         @{
         $self->sql_helper()->execute_simple( -SQL => 'show databases' )
         } };
}

sub is_view {
  my ( $self, $table ) = @_;
  my $view = 0;
  $self->sql_helper()->execute_no_return(
    -SQL =>
      sprintf( 'SHOW FULL TABLES FROM `%s` like ?', $self->database() ),
    -PARAMS   => [$table],
    -CALLBACK => sub {
      my ($row) = @_;
      $view = ( $row->[1] =~ /view/xmsi ) ? 1 : 0;
    } );
  return $view;
}

sub run_mysql_cmd {
  my ( $self, $sql ) = @_;
  my $dbc              = $self->target_dbc();
  my $mysql_login_args = $self->get_mysql_opts();
  my $database         = $self->database();
  my ( $fh, $filename ) = tempfile();
  print $fh $sql;
  close $fh;
  system("mysql $mysql_login_args $database < $filename") and
    $self->throw("Cannot issue $sql to mysql and DB ${database}: $!");
  return;
}

sub get_mysql_opts {
  my ($self) = @_;
  my $dbc = $self->target_dbc();
  my %args = ( host => $dbc->host(),
               port => $dbc->port(),
               user => $dbc->username() );
  $args{password} = $dbc->password() if $dbc->password();
  #Turns the above into --host=localhost --port=3306
  return
    join( q{ },
          map { sprintf( q{--%s='%s'}, $_, $args{$_} ) } keys %args );
}

1;

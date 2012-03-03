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
  return $self->param('target_dbc') if $self->param('target_dbc');
  my $details = $self->param('target_db');
  my $dbc = Bio::EnsEMBL::DBSQL::DBConnection->new(%{$details}, -reconnect_if_lost => 1);
  return $self->param('target_dbc', $dbc);
}

sub switch_db {
  my ($self, $db) = @_;
  $self->target_dbc()->disconnect_if_idle();
  $self->target_dbc()->dbname($db);
  return;
}

sub database {
  my ($self) = @_;
  return $self->param('directory')->[-1];
}

sub db_hash {
  my ($self) = @_;
  return {
    map { $_ => 1}
    @{$self->sql_helper()->execute_simple(-SQL => 'show databases')}
  };
}

sub is_view {
  my ($self, $table) = @_;
  my $view = 0;
  $self->sql_helper()->execute_no_return(
    -SQL => sprintf('SHOW FULL TABLES FROM `%s` like ?', $self->_database()),
    -PARAMS => [$table],
    -CALLBACK => sub {
      my ($row) = @_;
      $view = ($row->[1] =~ /view/xmsi) ? 1 : 0;
    }
  );
}

sub run_mysql_cmd {
  my ($self, $sql) = @_;
  my $dbc = $self->target_dbc();
  my $mysql_login_args = $self->get_mysql_opts();
  my $database = $self->database();
  my ($fh, $filename) = tempfile();
  print $fh $sql;
  close $fh;
  system("mysql $mysql_login_args $database < $filename")
    and throw("Cannot issue $sql to mysql and DB ${database}: $!");
  return;
}

sub get_mysql_opts {
  my ($self) = @_;
  my $dbc = $self->target_dbc();
  my %args = ( host => $dbc->host(), port => $dbc->port(), user => $dbc->username());
  $args{password} = $dbc->password() if $dbc->password();
  #Turns the above into --host=localhost --port=3306
  return join(q{ }, map { sprintf(q{--%s='%s'}, $_, $args{$_}) } keys %args);
}

1;
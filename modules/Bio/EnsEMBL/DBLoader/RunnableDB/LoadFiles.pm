package Bio::EnsEMBL::DBLoader::RunnableDB::LoadFiles;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base/;
use Bio::EnsEMBL::DBSQL::DBConnection;
use Bio::EnsEMBL::Utils::SqlHelper;
use Bio::EnsEMBL::Utils::Exception qw/throw/;
use Bio::EnsEMBL::Utils::Scalar qw/wrap_array/;

use Cwd;
use File::Temp qw(tempfile);
use IO::File;
use IO::Scalar;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
use File::Spec;

sub fetch_input {
  my ($self) = @_;
  my $local_dir = $self->local_dir();
  my $sql_filename = sprintf('%s.sql.gz', $self->_database());
  $self->param('sql_filename', $sql_filename);
  return;
}

sub run {
  my ($self) = @_;

  my $cwd = cwd();

  if($self->_db_exists()) {
    throw "The database already exists. Remove and retry";
  }
  $self->_create_db();
  $self->_switch_db();
  chdir($self->local_dir()) or throw 'Cannot change to '.$self->local_dir();

  $self->_load_sql();
  my %files = $self->_dump_files();
  foreach my $table (sort keys %files) {
    print STDERR "Processing $table\n" if $self->debug();
    $self->_load_data_file($table, $files{$table});
  }

  chdir($cwd) or throw 'Cannot change to '.$cwd;

  return;
}

sub _switch_db {
  my ($self) = @_;
  my $db = $self->_database();
  $self->_dbc()->disconnect_if_idle();
  $self->_dbc()->dbname($db);
  return;
}

sub _database {
  my ($self) = @_;
  return $self->param('directory')->[-1];
}

sub _db_exists {
  my ($self) = @_;
  return ($self->_db_hash()->{$self->_database()}) ? 1 : 0;
}

#Filters SQL into two files; 1 with tables & the other with views
sub _load_sql {
  my ($self) = @_;
  my $sql = $self->param('sql_filename');
  if(! -f $sql) {
    throw "Cannot find the expected SQL file $sql";
  }
  my $file = $self->_gunzip_file($sql);

  #Splitting view definitions into a separate file
  my $table_sql = $self->_filter_file($file, sub {
    my ($line) = @_;
    return ( $line !~ /CREATE ALGORITHM/ ) ? 1 : 0;
  });
  $self->_run_mysql_cmd($table_sql);

  my $view_sql = $self->_filter_file($file, sub {
    my ($line) = @_;
    return ( $line =~ /CREATE ALGORITHM/ ) ? 1 : 0;
  }, sub {
    my ($line) = @_;
    $line =~ s/DEFINER=.+ \s+ SQL \s+ SECURITY \s+ DEFINER/SQL SECURITY INVOKER/xms;
    return $line;
  });

  if($view_sql) {
    $self->_run_mysql_cmd($view_sql);
  }

  return;
}

#Loads everything from a file
sub _load_data_file {
  my ($self, $table, $files) = @_;

  my $is_view = $self->_is_view($table);

  if($is_view) {
    print STDERR "\tSkipping $table as it is a VIEW not a table\n" if $self->debug();
    return;
  }

  my $target_file = "${table}.txt";

  $self->_gunzip_file($files, $target_file);

  print STDERR "\tLoading the table $table\n" if $self->debug();
  $self->_disable_indexes($table);

  my $force_escape = q{FIELDS ESCAPED BY '\\\\'};
  my $sql = qq|LOAD DATA LOCAL INFILE '${target_file}' INTO TABLE `${table}` ${force_escape}|;
  $self->_dbc()->do($sql);

  $self->_enable_indexes($table);
  $self->_optimize_table($table);
  print STDERR "\tFinished processing $table\n" if $self->debug();

  unlink($target_file);
  return;
}

sub _db_hash {
  my ($self) = @_;
  return {
    map { $_ => 1}
    @{$self->_sql_helper()->execute_simple(-SQL => 'show databases')}
  };
}

sub _is_view {
  my ($self, $table) = @_;
  my $view = 0;
  $self->_sql_helper()->execute_no_return(
    -SQL => sprintf('SHOW FULL TABLES FROM `%s` like ?', $self->_database()),
    -PARAMS => [$table],
    -CALLBACK => sub {
      my ($row) = @_;
      $view = ($row->[1] =~ /view/xmsi) ? 1 : 0;
    }
  );
}

sub _create_db {
  my ($self) = @_;
  my $db = $self->_database();
  $self->_dbc()->do("create database `$db`");
  return;
}

sub _disable_indexes {
  my ($self, $table) = @_;
  $self->_dbc()->do("alter table `${table}` disable keys");
  return;
}

sub _enable_indexes {
  my ($self, $table) = @_;
  $self->_dbc()->do("alter table `${table}` enable keys");
  return;
}

sub _optimize_table {
  my ($self, $table) = @_;
  $self->_dbc()->do("optimize table `${table}`");
  return;
}

sub _run_mysql_cmd {
  my ($self, $sql) = @_;
  my $dbc = $self->_dbc();
  my $mysql_login_args = $self->_get_mysql_opts();
  my $database = $self->_database();
  my ($fh, $filename) = tempfile();
  print $fh $sql;
  close $fh;
  system("mysql $mysql_login_args $database < $filename")
    and throw("Cannot issue $sql to mysql and DB ${database}: $!");
  return;
}

sub _get_mysql_opts {
  my ($self) = @_;
  my $dbc = $self->_dbc();
  my %args = ( host => $dbc->host(), port => $dbc->port(), user => $dbc->username());
  $args{password} = $dbc->password() if $dbc->password();
  #Turns the above into --host=localhost --port=3306
  return join(q{ }, map { sprintf(q{--%s='%s'}, $_, $args{$_}) } keys %args);
}

sub _sql_helper {
  my ($self) = @_;
  return $self->_dbc()->sql_helper();
}

sub _dbc {
  my ($self) = @_;
  return $self->param('dbc') if $self->param('dbc');
  my $details = $self->param('target_db');
  my $dbc = Bio::EnsEMBL::DBSQL::DBConnection->new(%{$details}, -reconnect_if_lost => 1);
  return $self->param('dbc', $dbc);
}

sub _gunzip_file {
  my ($self, $files, $target) = @_;
  $files = wrap_array($files);

  my $target_file = $target || $files->[0];
  $target_file =~ s/\.gz$//xms;
  unlink $target_file if -f $target_file;

  print STDERR sprintf("Decompressing [%s] to %s\n", join(q{, }, @{$files}), $target) if $self->debug();
  foreach my $file (@{$files}) {
    gunzip $file => $target_file, Append => 1 or throw ("gunzip failed from $file to $target : $GunzipError\n");
  }
  return $target_file;
}

sub _filter_file {
  my ($self, $input_file, $filter, $alter) = @_;
  my $output;
  my $output_fh = IO::Scalar->new(\$output);
  my $found_lines = 0;
  open (my $input_fh, '<', $input_file) or throw "Cannot open file $input_file: $!";
  while( my $line = <$input_fh>) {
    if($filter->($line)) {
      $found_lines = 1;
      $line = $alter->($line) if defined $alter;
      print $output_fh $line;
    }
  }
  close $input_fh;
  close $output_fh;
  return $output;
}

sub _dump_files {
  my ($self) = @_;
  my $dir = cwd();
  opendir(my $dh, $dir) or throw("Cannot open ${dir}: $!");
  my @files = grep {
    my $f = $_;
    $f =~ /\.txt\.gz$/;
  } readdir($dh);
  closedir($dh);

  # This is attempting to work with the old E! system where tables were split
  # amongst multiple files. We need to strip out possible digits & the
  # .txt.gz. All files are stored by their table name to aid loading
  my %file_hash;
  foreach my $file (@files) {
    #Tables can be alpha-numeric, underscores and even have a space
    $file =~ /([A-Za-z_0-9 ]+)(?:\.\d+)?\.txt\.gz/xms;
    my $table_name = $1;
    $file_hash{$table_name} = [] if ! exists $file_hash{$table_name};
    push(@{$file_hash{$table_name}}, $file);
  }

  foreach my $key (keys %file_hash) {
    my $data = $file_hash{$key};
    my @sorted_data = sort { $a cmp $b } @{$data};
    $file_hash{$key} = \@sorted_data;
  }

  return %file_hash;
}

1;
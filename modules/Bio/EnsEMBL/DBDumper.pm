package Bio::EnsEMBL::DBDumper;

use strict;
use warnings;

use Bio::EnsEMBL::Utils::Argument qw/rearrange/;
use Bio::EnsEMBL::Utils::Exception qw/throw warning/;
use Bio::EnsEMBL::Utils::IO qw/work_with_file/;
use Bio::EnsEMBL::Utils::Scalar qw/:assert/;
use Bio::EnsEMBL::DBDumper::Data;
use Bio::EnsEMBL::DBDumper::Sql;
use IO::Compress::Gzip qw/gzip $GzipError/;
use File::Path qw/mkpath/;

sub new {
  my ($class, @args) = @_;
  $class = ref($class) || $class;
  my ($tables, $dbc, $base_dir) = rearrange([qw/tables dbc base_dir/], @args);
  
  assert_ref($dbc, 'Bio::EnsEMBL::DBSQL::DBConnection', '-DBC');
  throw "No -BASE_DIR given" unless $base_dir;
  throw "-BASE_DIR $base_dir does not exist" unless -d $base_dir;
  
  my $self = bless({ dbc => $dbc, base_dir => $base_dir }, $class);
  $self->tables($tables) if $tables;
  
  return $self;
}

sub run {
  my ($self) = @_;
  $self->_dump_sql();
  $self->_sql_output()->close();
  $self->_dump_data();
  $self->_checksum();
  return;
}

sub _dump_data {
  my ($self) = @_;
  my $objs = $self->_get_data_objects();
  my ($tables, $views) = $self->_filter_views($objs);
  $self->_run_objs($tables);
  return;
}

sub _dump_sql {
  my ($self) = @_;
  my $objs = $self->_get_sql_objects();
  my ($tables, $views) = $self->_filter_views($objs);
  $self->_run_objs($tables);
  $self->_run_objs($views);
  return;
}

sub _run_objs {
  my ($self, $objs, $type) = @_;
  foreach my $t (@{$objs}) {
    $t->run();
  }
  return;
}

sub _checksum {
  my ($self) = @_;
  my $dir = $self->dir();
  opendir(my $dh, $dir) or throw "Cannot open directory $dir";
  my @files = sort { $a cmp $b } readdir($dh);
  closedir($dh) or throw "Cannot close directory $dir";
  
  my $checksum = File::Spec->catfile($dir, 'CHECKSUMS');
  work_with_file($checksum, 'w', sub {
    my ($fh) = @_;
    foreach my $file (@files) {
      next if $file =~ /^\./; #hidden or up dir
      my $path = File::Spec->catfile($dir, $file);
      my $sum = `sum $path`;
      $sum =~ s/\s* $path//xms; 
      print $fh $file, "\t", $sum;
    }
  });
  gzip $checksum => "${checksum}.gz" or throw "Cannot gzip checksum $checksum: $GzipError";
  unlink $checksum;
  
  return;
}

sub _get_data_objects {
  my ($self) = @_;
  my $dbc = $self->dbc();
  my $fh = $self->_sql_output();
  my $dir = $self->dir();
  my @objs;
  foreach my $t (@{$self->tables()}) {
    my $o = Bio::EnsEMBL::DBDumper::Data->new( 
      -NAME => $t, -FILE => File::Spec->catfile($dir, "${t}.txt"), -DBC => $dbc, -COMPRESS => 1 
    );
    push(@objs, $o);
  }
  return \@objs;
}

sub _get_sql_objects {
  my ($self) = @_;
  my $dbc = $self->dbc();
  my $fh = $self->_sql_output();
  return [ map { Bio::EnsEMBL::DBDumper::Sql->new( -NAME => $_, -FH => $fh, -DBC => $dbc ) } @{$self->tables()} ];
}

sub _filter_views {
  my ($self, $objects) = @_;
  my @tables;
  my @views;
  foreach my $t (@{$objects}) {
    if($t->is_view()) {
      push(@views, $t);
    }
    else {
      push(@tables, $t);
    }
  }
  return ( \@tables, \@views);
}

sub _sql_output {
  my ($self, $_sql_output) = @_;
  if(! exists $self->{'_sql_output'} && ! defined $_sql_output) {
    my $db = $self->dbc()->dbname();
    my $file = File::Spec->catfile($self->dir(), "${db}.sql.gz");
    my $fh = IO::Compress::Gzip->new($file) or throw "Could not open Gzip to $file: $GzipError";
    $_sql_output = $fh;
  }
  if(defined $_sql_output) {
    assert_file_handle($_sql_output, '_sql_output');
  	$self->{'_sql_output'} = $_sql_output;
  }
  return $self->{'_sql_output'};
}

sub dbc {
  my ($self) = @_;
  return $self->{'dbc'};
}

sub tables {
  my ($self, $tables) = @_;
  if(! exists $self->{'tables'} && ! defined $tables) {
    $tables = $self->dbc()->sql_helper()->execute_simple(-SQL => 'show tables');
    $tables = [sort { $a cmp $b } @{$tables}];
  }
  if(defined $tables) { 
    assert_ref($tables, 'ARRAY', 'tables');
  	$self->{'tables'} = $tables;
  }
  return $self->{'tables'};
}

sub base_dir {
  my ($self) = @_;
  return $self->{'base_dir'};
}

sub dir {
  my ($self) = @_;
  my $dir = File::Spec->catdir($self->base_dir(), $self->dbc()->dbname());
  if(! -d $dir) {
    mkpath $dir or throw "Cannot create the directory $dir: $!";
    chmod(0777, $dir) or throw "Cannot change permissions on dir for everyone to write: $!";
  }
  return $dir;
}


1;
package Bio::EnsEMBL::DBDumper;

use strict;
use warnings;

use Bio::EnsEMBL::Utils::Exception qw/throw warning/;
use Bio::EnsEMBL::Utils::Scalar qw/:assert/;
use Bio::EnsEMBL::DBDumper::Data;
use Bio::EnsEMBL::DBDumper::Sql;
use IO::Compress::Gzip qw/$GzipError/;
use File::Path qw/mkpath/;

sub new {
  my ($class, @args) = @_;
  $class = ref($class) || $class;
  my ($tables, $dbc, $dir) = rearrange([qw/tables dbc dir/], @args);
  
  assert_ref($dbc, 'Bio::EnsEMBL::DBSQL::DBConnection', '-DBC');
  throw "No -DIR given" unless $dir;
  throw "-DIR $dir does not exist" unless -d $dir;
  
  my $self = bless({ dbc => $dbc, dir => $dir }, $class);
  $self->tables($tables) if $tables;
  
  return $self;
}

sub run {
  my ($self) = @_;
  $self->_dump_sql();
  $self->_dump_data();
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

sub _get_data_objects {
  my ($self) = @_;
  my $dbc = $self->dbc();
  my $fh = $self->_sql_output();
  return [ map { Bio::EnsEMBL::DBDumper::Data->new( -NAME => $_, -FH => $fh, -DBC => $dbc ) } sort { $a cmp $b } @{$self->tables()} ];
}

sub _get_sql_objects {
  my ($self) = @_;
  my $dbc = $self->dbc();
  my $fh = $self->_sql_output();
  return [ map { Bio::EnsEMBL::DBDumper::Sql->new( -NAME => $_, -FH => $fh, -DBC => $dbc ) } sort { $a cmp $b } @{$self->tables()} ];
}

sub _filter_views {
  my ($self, $objects) = @_;
  my @views;
  my @tables;
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
    assert_ref($_sql_output, 'type', '_sql_output');
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
    $tables = $self->dbc()->simple_result(-SQL => 'show tables');
  }
  if(defined $tables) { 
    assert_ref($tables, 'ARRAY', 'tables');
  	$self->{'tables'} = $tables;
  }
  return $self->{'tables'};
}


sub dir {
  my ($self) = @_;
  my $dir = $self->{'dir'};
  if(! -d $dir) {
    mkpath $dir or throw "Cannot create the directory $dir: $!";
  }
  return $dir;
}


1;
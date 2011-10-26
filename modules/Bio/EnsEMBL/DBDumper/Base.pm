package Bio::EnsEMBL::DBDumper::Base;

use strict;
use warnings;

use Bio::EnsEMBL::Utils::Argument qw/rearrange/;
use Bio::EnsEMBL::Utils::Exception qw/throw warning/;
use Bio::EnsEMBL::Utils::Scalar qw/:assert/;

sub new {
  my ($class, @args) = @_;
  $class = ref($class) || $class;
  my ($name, $dbc, $fh, $file) = rearrange([qw/name dbc fh file/], @args);
  
  throw "-NAME is a required parameter" unless $name;
  assert_ref($dbc, 'Bio::EnsEMBL::DBSQL::DBConnection', '-DBC');
  
  my $self = bless({ name => $name, dbc => $dbc }, $class);
  $self->fh($fh) if $fh;
  $self->file($file) if $file;
  return $self;
}

sub name {
  my ($self) = @_;
  return $self->{'name'};
}

sub fh {
  my ($self, $fh) = @_;
  if(defined $fh) {
    assert_file_handle($fh, 'file handle');
  	$self->{'fh'} = $fh;
  }
  return $self->{'fh'};
}

sub file {
  my ($self, $file) = @_;
  $self->{'file'} = $file if defined $file;
  return $self->{'file'};
}

sub dbc {
  my ($self) = @_;
  return $self->{'dbc'};
}

sub is_view {
  my ($self) = @_;
  my $table_type = $self->dbc()->sql_helper()->execute_single_result(
    -SQL => q{select TABLE_TYPE from information_schema.TABLES where TABLE_NAME =? and TABLE_SCHEMA =?},
    -PARAMS => [ $self->name(), $self->dbc()->dbname() ]
  );
  return ( $table_type eq 'VIEW' ) ? 1 : 0;
}

1;
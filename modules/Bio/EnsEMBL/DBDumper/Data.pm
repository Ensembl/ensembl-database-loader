package Bio::EnsEMBL::DBDumper::Data;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBDumper::Base/;

use Bio::EnsEMBL::Utils::Argument qw/rearrange/;
use IO::Compress::Gzip qw(gzip $GzipError);

sub new {
  my ($class, @args) = @_;
  my $self = $class->SUPER::new(@args);
  my ($compress) = rearrange([qw/compress/], @args);
  $self->compress($compress);
  return;
}

sub run {
  my ($self) = @_;
  $self->_into_outfile();
  $self->_gzip_output();
  return;
}

sub _into_outfile {
  my ($self) = @_;
  my ($q_table) = @{$self->dbc()->quote_identifier($self->name())};
  my $file = $self->file();
  my $force_escape = q{FIELDS OPTIONALLY ESCAPED BY '\\\\'};
  my $sql = qq{SELECT * FROM $q_table INTO OUTFILE ? $force_escape};
  $self->dbc()->sql_helper()->execute_no_return(-SQL => $sql, -PARAMS => [$file]);
  return;
}

sub _gzip_output {
  my ($self) = @_;
  return unless $self->compress();
  my $file = $self->file();
  my $target_file = $file.'.gz';
  gzip $file => $target_file or throw ("gzip failed from $file to $target_file : $GzipError\n");
  return;
}

sub compress {
  my ($self, $compress) = @_;
  if(! exists $self->{'compress'} && ! defined $compress) {
    $compress = 1;
  }
  if(defined $compress) {
  	$self->{'compress'} = $compress;
  }
  return $self->{'compress'};
}


1;
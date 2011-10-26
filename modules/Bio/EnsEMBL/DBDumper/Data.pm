package Bio::EnsEMBL::DBDumper::Data;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBDumper::Base/;

use Bio::EnsEMBL::Utils::Argument qw/rearrange/;
use Bio::EnsEMBL::Utils::Exception qw/throw/;
use IO::Compress::Gzip qw(gzip $GzipError);

sub new {
  my ($class, @args) = @_;
  my $self = $class->SUPER::new(@args);
  my ($compress) = rearrange([qw/compress/], @args);
  $self->compress($compress);
  return $self;
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
  my $force_escape = q{FIELDS ESCAPED BY '\\\\'};
  my $sql = sprintf(q{SELECT * FROM %s INTO OUTFILE '%s' %s}, $q_table, $file, $force_escape);
  $self->dbc()->do($sql);
  return;
}

sub _gzip_output {
  my ($self) = @_;
  return unless $self->compress();
  my $file = $self->file();
  my $target_file = $file.'.gz';
  if(-f $target_file) {
    unlink $target_file or throw "Cannot remove the existing gzip file $target_file: $!";
  }
  gzip $file => $target_file or throw ("gzip failed from $file to $target_file : $GzipError\n");
  if(-f $target_file) {
    unlink $file or throw "Cannot remove the file $file: $!";
  }
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
package Bio::EnsEMBL::DBLoader::RunnableDB::Prioritise;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base/;

sub param_defaults {
  my ($self) = @_;
  return {
    priority => { species => [], group => [] }
  };
}

sub run {
  my ($self) = @_;

  my $directory = $self->param('directory');
  my $db_name = $directory->[-1];
  my $class = $directory->[-2];

  if($class eq 'data_files'){
    my $path = join(q{/}, @{$directory});
    $self->warning("The directory $path contains 'data_files'; will no longer continue to load into a database");
    return;
  }

  my $priority = 0;

  foreach my $species (@{$self->param('priority')->{species}}) {
    if($db_name =~ /^$species/xms) {
      $priority = 1;
      $self->warning("DB name ${db_name} matched the prioritised species ${species}");
      last;
    }
  }
  if(!$priority) {
    foreach my $group (@{$self->param('priority')->{group}}) {
      if($db_name =~ /_${group}_/xms) {
        $priority = 1;
        $self->warning("DB name ${db_name} matched the prioritised group ${group}");
        last;
      }
    }
  }

  $self->param('priority', $priority);

  return;
}

sub write_output {
  my ($self) = @_;
  my $dataflow = $self->param('priority') ? 2 : 3;
  $self->dataflow_output_id({ directory => $self->param('directory') }, $dataflow);
  return;
}

1;
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

  my $database = $self->param('database');
  my $priority = 0;

  foreach my $species (@{$self->param('priority')->{species}}) {
    if($database =~ /^$species/xms) {
      $priority++;
      $self->warning("DB name ${database} matched the prioritised species ${species}");
      last;
    }
  }
  if(!$priority) {
    foreach my $group (@{$self->param('priority')->{group}}) {
      if($database =~ /_${group}_/xms) {
        $priority++;
        $self->warning("DB name ${database} matched the prioritised group ${group}");
        last;
      }
    }
  }

  $self->param('priority', $priority);

  return;
}

sub write_output {
  my ($self) = @_;
  my $priority_to_flow = {
    0 => 2, #basic flow
    1 => 3, #higher
    2 => 4, #highest
  };
  my $dataflow = $priority_to_flow->{$self->param('priority')};
  $self->dataflow_output_id({ database => $self->param('database') }, $dataflow);
  return;
}

1;
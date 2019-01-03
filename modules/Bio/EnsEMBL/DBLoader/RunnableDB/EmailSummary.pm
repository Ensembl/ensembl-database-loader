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

package Bio::EnsEMBL::DBLoader::RunnableDB::EmailSummary;

use strict;
use warnings;
use base qw/Bio::EnsEMBL::Hive::RunnableDB::NotifyByEmail/;
use Bio::EnsEMBL::Hive::Utils qw/destringify/;

=pod

=head1 CONTACT

  Please email comments or questions to the public Ensembl
  developers list at <http://lists.ensembl.org/mailman/listinfo/dev>.

  Questions may also be sent to the Ensembl help desk at
  <http://www.ensembl.org/Help/Contact>.

=head1 NAME

Bio::EnsEMBL::DBLoader::RunnableDB::EmailSummary

=head1 DESCRIPTION

Package responsible for sending a summary email once the pipeline has successfully completed.

Allowed parameters are:

=over 8

=item email - The user email address

=item subject - The subject of the email, containing the database name

=back

=cut

sub fetch_input {
  my ($self) = @_;
  
  $self->assert_executable('sendmail');
  
  my $download = $self->jobs('download');
  my $prioritise = $self->jobs('prioritise');
  my $human_variation_load_files = $self->jobs('human_variation_load_files');
  my $super_priority_load_files = $self->jobs('super_priority_load_files');
  my $high_priority_load_files = $self->jobs('high_priority_load_files');
  my $load_files = $self->jobs('load_files');
  my $grant = $self->jobs('grant');
    
  my @args = (
    $download->{successful_jobs},
    $download->{failed_jobs},
    $prioritise->{successful_jobs},
    $prioritise->{failed_jobs},
    $human_variation_load_files->{successful_jobs},
    $human_variation_load_files->{failed_jobs},
    $super_priority_load_files->{successful_jobs},
    $super_priority_load_files->{failed_jobs},
    $high_priority_load_files->{successful_jobs},
    $high_priority_load_files->{failed_jobs},
    $load_files->{successful_jobs},
    $load_files->{failed_jobs},
    $grant->{successful_jobs},
    $grant->{failed_jobs},
    $self->failed(),
  );
  
  my $msg = sprintf(<<'MSG', @args);
Your Ensembl Mirrors Pipeline has finished. We have:

  * %d databases successfully downloaded (%d failed)
  * %d databases successfully prioritised (%d failed)
  * %d human variation database successfully loaded (%d failed)
  * %d super priority databases successfully loaded (%d failed)
  * %d high priority databases successfully loaded (%d failed)
  * %d databases successfully loaded (%d failed)
  * %d databases were successfully granted access (%d failed)

%s

MSG
  $self->param('text', $msg);
  return;
}

sub jobs {
  my ($self, $logic_name, $minimum_runtime) = @_;
  my $aa = $self->db->get_AnalysisAdaptor();
  my $aja = $self->db->get_AnalysisJobAdaptor();
  my $analysis = $aa->fetch_by_logic_name($logic_name);
  my $id = $analysis->dbID();
  my @jobs = @{$aja->fetch_all_by_analysis_id($id)};
  $_->{input} = destringify($_->input_id()) for @jobs;
  @jobs = 
    sort { $a->{input}->{database} cmp $b->{input}->{database} }
    grep { 
      if($minimum_runtime) {
        if($minimum_runtime > $_->runtime_msec()) {
          1;
        }
        else {
          0;
        }
      }
      else {
        1;
      }
    }
    @jobs;
  my %passed_database = map { $_->{input}->{database}, 1 } grep { $_->status() eq 'DONE' } @jobs;
  my %failed_database = map { $_->{input}->{database}, 1 } grep { $_->status() eq 'FAILED' } @jobs;
  return {
    analysis => $analysis,
    name => $logic_name,
    jobs => \@jobs,
    successful_jobs => scalar(keys %passed_database),
    failed_jobs => scalar(keys %failed_database),
  };
}


sub failed {
  my ($self) = @_;
  my $failed;
  if ($self->db()->get_AnalysisJobAdaptor()->can('fetch_all_by_analysis_id_status') ) {
      $failed = $self->db()->get_AnalysisJobAdaptor()->fetch_all_by_analysis_id_status(undef,'FAILED');
  } elsif ($self->db()->get_AnalysisJobAdaptor()->can('fetch_all_failed_jobs') ) {
      $failed = $self->db()->get_AnalysisJobAdaptor()->fetch_all_failed_jobs();
  } else {
      $self->throw("The failed analysis lookup method in Hive has changed again.");
  }
  if(! @{$failed}) {
    return 'No jobs failed. Congratulations!';
  }
  my $output = <<'MSG';
The following jobs have failed during this run. Please check your hive's error msg table for the following jobs:

MSG
  foreach my $job (@{$failed}) {
    my $analysis = $self->db()->get_AnalysisAdaptor()->fetch_by_dbID($job->analysis_id());
    my $line = sprintf(q{  * job_id=%d %s(%5d) input_id='%s'}, $job->dbID(), $analysis->logic_name(), $analysis->dbID(), $job->input_id());
    $output .= $line;
    $output .= "\n";
  }
  return $output;
}

my $sorter = sub {
  my $status_to_int = sub {
    my ($v) = @_;
    return ($v->status() eq 'FAILED') ? 0 : 1;
  };
  my $status_sort = $status_to_int->($a) <=> $status_to_int->($b);
  return $status_sort if $status_sort != 0;
  return $a->{input}->{database} cmp $b->{input}->{database};
};

sub assert_executable {
  my ($self, $exe) = @_;
  if(! -x $exe) {
    my $output = `which $exe 2>&1`;
    chomp $output;
    my $rc = $? >> 8;
    if($rc != 0) {
      my $possible_location = `locate -l 1 $exe 2>&1`;
      my $loc_rc = $? >> 8;
      if($loc_rc != 0) {
        my $msg = 'Cannot find the executable "%s" after trying "which" and "locate -l 1". Please ensure it is on your PATH or use an absolute location and try again';
        $self->throw(sprintf($msg, $exe));
      }
    }
  }
  return 1;
}


1;

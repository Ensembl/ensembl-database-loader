
=head1 LICENSE

Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
Copyright [2016-2025] EMBL-European Bioinformatics Institute

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

package Bio::EnsEMBL::DBLoader::RunnableDB::Grant;

use strict;
use warnings;

=pod

=head1 CONTACT

  Please email comments or questions to the public Ensembl
  developers list at <http://lists.ensembl.org/mailman/listinfo/dev>.

  Questions may also be sent to the Ensembl help desk at
  <http://www.ensembl.org/Help/Contact>.

=head1 NAME

Bio::EnsEMBL::DBLoader::RunnableDB::Grant

=head1 DESCRIPTION

Package responsible for granting access to loaded databases. This prevents users
from accidentally querying a database before they are meant to (a common problem
when using global grants).

Allowed parameters are:

=over 8

=item database - The database to perform grants on

=item grant_user - The user to grant to. We only grant to % so socket based connections will not get the grant

=item target_db - HashRef of DBConnection compatible settings which are piped directly into a DBConnection->new() call

=back

=cut

use base
  qw/Bio::EnsEMBL::DBLoader::RunnableDB::Base Bio::EnsEMBL::DBLoader::RunnableDB::Database/;
use Bio::EnsEMBL::Utils::Scalar qw/wrap_array/;

sub param_defaults {
  return { grant_users => [ 'anonymous', 'ensro' ], };
}

sub fetch_input {
  my ($self) = @_;
  $self->throw('No database given') unless $self->database();
  $self->throw('No grant_user given')
    unless $self->param('grant_users');
  return;
}

sub run {
  my ($self) = @_;
  if ( !$self->param('prerelease') ) {
    my $grant_template =
      q{GRANT SELECT, EXECUTE ON `%s`.* TO '%s'@'%%'};
    my $database    = $self->database();
    my $grant_users = wrap_array( $self->_get_users() );
    my @ddl;
    foreach my $grant_user ( @{$grant_users} ) {
      my $grant_ddl =
        sprintf( $grant_template, $database, $grant_user );
      $self->warning($grant_ddl);
      push( @ddl, $grant_ddl );
    }
    $self->param( 'ddl', \@ddl );
  }
  return;
}

sub write_output {
  my ($self) = @_;
  if ( !$self->param('prerelease') ) {
    foreach my $ddl ( @{ $self->param('ddl') } ) {
      $self->target_dbc()->do($ddl);
    }
    $self->target_dbc()->do('flush privileges');
  }
  return;
}

sub _get_users {
  my ($self) = @_;
  if ( $self->param_is_defined('user_submitted_grant_users') ) {
    my $grants = $self->param('user_submitted_grant_users');
    if ( @{$grants} ) {
      return $grants;
    }
  }
  return $self->param('grant_users');
}

1;

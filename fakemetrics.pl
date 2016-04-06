#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use Time::HiRes qw(usleep);

my $test;
GetOptions (
    t => \$test,
);

my $mt = UdpSegment->new() or die "Cannot create UDP instance object: $@";
my @applications = qw(inv);
my @hosts = qw(
	web01 web02 web03 web04 web05 web06 web07 web08 web09 web10
	prc01 prc02 prc03 prc04 prc05 prc06 prc07 prc08
);
my @request_sources = qw(apache systask);
my $layers = {
	data => {
		mal_e => {
			analytics_listing_select => [qw(elapsed)]
		},
		mongo => {
			__valuation__select => [qw(elapsed)]
		},
		kard => {
			transfer_report => [qw(elapsed)]
		},
	},
	logic => {
		deployment => {
			started => [qw(count)],
			ended => [qw(count)],
		},
		dtv => {
			retrieved_configuration => [qw(count)],
		},
		ebay => {
			listing_sent => [qw(elapsed)],
			system_task_processed => [qw(count)],
		},
		incoming_feeds => {
			processed_feed_file => [qw(elapsed)],
			processed_feed_set => [qw(elapsed)],
		},
		inventory => {
			'save-created' => [qw(count)],
			'save-updated' => [qw(count)],
		},
		media => {
			'save-created' => [qw(count)],
		},
	},
};

for (my $i = 0; $i <= 10000; $i++) {
	my $application = $applications[rand @applications];
	my $host = $hosts[rand @hosts];
	my $request_source = $request_sources[rand @request_sources];
	my $layer = (keys %$layers)[rand keys %$layers];
	my $source = (keys %{$layers->{$layer}})[rand keys %{$layers->{$layer}}];
	my $sub_source = (keys %{$layers->{$layer}{$source}})[rand keys %{$layers->{$layer}{$source}}];
	my $metric_type = $layers->{$layer}{$source}{$sub_source}[rand @{$layers->{$layer}{$source}{$sub_source}}];

	my $metric_name = join(
		'.',
		$application, $host, $request_source, $layer, $source, $sub_source, $metric_type
	);

	my $metric_value;
	if ($metric_type eq 'count') {
		#$metric_value = ( $i % 2 ) ? -1 : 1;
		$metric_value = 1;
	} else {
		$metric_value = rand(1);
	}

	if ($test) {
		print STDERR "test mode: $metric_name = $metric_value\n";
	} else {
		$mt->record($metric_name, $metric_value);
	}
	usleep(1000);
}

{
	package UdpSegment;

	use strict;
	use warnings;

	use IO::Socket::INET;

	use constant DEFAULT_SERVER_PORT => 2003;
	use constant DEFAULT_SERVER_ADDR => '127.0.0.1';

	my $has_failed = 0;

	sub new {
		my ($class, %args) = @_;

		my $self = {
			_port => $args{port} || DEFAULT_SERVER_PORT,
			_host => $args{host} || DEFAULT_SERVER_ADDR,
		};

		return bless $self, $class;
	}

	sub record {
		my ($self, $key, $value) = @_;
		return 0 unless defined $key;

		return $self->_send_datagram($key, $value);
	}

	sub _send_datagram {
		my ($self, $path, $value) = @_;

		my $datum         = "$path $value " . time();
		my $expected_size = length($datum);
		my $actual_size   = $self->_get_socket()->send($datum);    # throws Error
		print "Sent: $datum\n";

		if (!$actual_size) {
			die 'Datagram send failed because ' . $!;
		} elsif ($expected_size != $actual_size) {
			die sprintf(
				'Incomplete datagram %s=%s (%i of %i bytes transmitted)',
				$path, $value, $actual_size, $expected_size
			);
		}

		return 1;
	}

	sub _get_socket {
		my ($self, %args) = @_;

		unless ($self->{socket}) {
			$self->{socket} = IO::Socket::INET->new(
				Proto    => 'udp',
				PeerPort => $self->{_port},
				PeerAddr => $self->{_host},
			) or die "Can't create server: $@";
		}

		return $self->{socket};
	}
}


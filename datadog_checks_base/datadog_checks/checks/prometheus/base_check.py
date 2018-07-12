# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

from .mixins import PrometheusScraperMixin

from .. import AgentCheck
from ...errors import CheckException


class PrometheusScraper(PrometheusScraperMixin):
    """
    This class scrapes a prometheus endpoint and submits the metrics on behalf of a check. This class
    is used by checks that scrape more than one prometheus endpoint.
    """

    def __init__(self, check):
        super(PrometheusScraper, self).__init__()
        self.check = check


class GenericPrometheusCheck(PrometheusScraperMixin, AgentCheck):
    """
    GenericPrometheusCheck is a class that helps instantiating PrometheusCheck only
    with YAML configurations. As each check has it own states it maintains a map
    of all checks so that the one corresponding to the instance is executed

    Minimal example configuration:
    instances:
    - prometheus_url: http://foobar/endpoint
        namespace: "foobar"
        metrics:
        - bar
        - foo
    """
    def __init__(self, name, init_config, agentConfig, instances=None, default_instances={}, default_namespace=None):
        super(GenericPrometheusCheck, self).__init__(name, init_config, agentConfig, instances)
        self.config_map = {}
        self.default_instances = default_instances
        self.default_namespace = default_namespace

        # Set up the config map
        for instance in instances:
            self.get_scraper_config(instance)

    def check(self, instance):
        endpoint = instance.get("prometheus_url", None)
        if endpoint is None:
            raise CheckException("Unable to find prometheus URL in config file.")

        scraper_config = self.get_scraper_config(endpoint, instance)
        if not scraper_config['metrics_mapper']:
            raise CheckException("You have to collect at least one metric from the endpoint: {}".format(endpoint))

        self.process(
            endpoint,
            scraper_config,
            ignore_unmapped=True
        )

    def get_scraper_config(self, endpoint, instance):
        # If we already created the corresponding scraper, return it
        if endpoint in self.config_map:
            return self.config_map[endpoint]

        # Otherwise we create the scraper
        config = self.create_mixin_configuration(instance, default_instances=self.default_instances,
                                                 default_namespace=self.default_namespace)
        self.config_map[endpoint] = config

        return config

    def _submit_rate(self, metric_name, val, metric, custom_tags=None, hostname=None):
        """
        Submit a metric as a rate, additional tags provided will be added to
        the ones from the label provided via the metrics object.

        `custom_tags` is an array of 'tag:value' that will be added to the
        metric when sending the rate to Datadog.
        """
        _tags = self._metric_tags(metric_name, val, metric, custom_tags, hostname)
        self.rate('{}.{}'.format(self.NAMESPACE, metric_name), val, _tags, hostname=hostname)

    def _submit_gauge(self, metric_name, val, metric, custom_tags=None, hostname=None):
        """
        Submit a metric as a gauge, additional tags provided will be added to
        the ones from the label provided via the metrics object.

        `custom_tags` is an array of 'tag:value' that will be added to the
        metric when sending the gauge to Datadog.
        """
        _tags = self._metric_tags(metric_name, val, metric, custom_tags, hostname)
        self.gauge('{}.{}'.format(self.NAMESPACE, metric_name), val, _tags, hostname=hostname)

    def _submit_monotonic_count(self, metric_name, val, metric, custom_tags=None, hostname=None):
        """ 
        Submit a metric as a monotonic count, additional tags provided will be added to
        the ones from the label provided via the metrics object.

        `custom_tags` is an array of 'tag:value' that will be added to the
        metric when sending the monotonic count to Datadog.
        """

        _tags = self._metric_tags(metric_name, val, metric, custom_tags, hostname)
        self.monotonic_count('{}.{}'.format(self.NAMESPACE, metric_name), val, _tags, hostname=hostname)

    def _metric_tags(self, metric_name, val, metric, scraper_config, hostname=None):
        custom_tags = scraper_config['custom_tags']
        _tags = list(custom_tags)
        for label in metric.label:
            if label.name not in scraper_config['exclude_labels']:
                tag_name = label.name
                if label.name in scraper_config['labels_mapper']:
                    tag_name = scraper_config['labels_mapper'][label.name]
                _tags.append('{}:{}'.format(tag_name, label.value))
        return self._finalize_tags_to_submit(_tags, metric_name, val, metric, custom_tags=custom_tags, hostname=hostname)

    def _submit_service_check(self, *args, **kwargs):
        self.service_check(*args, **kwargs)

    def _finalize_tags_to_submit(self, _tags, metric_name, val, metric, custom_tags=None, hostname=None):
        """
        Format the finalized tags
        This is generally a noop, but it can be used to hook into _submit_gauge and change the tags before sending
        """
        return _tags

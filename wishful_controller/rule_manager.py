import logging
from wishful_framework import TimeEvent, PktEvent, MovAvgFilter, PeakDetector, Match, Action, Permanance

__author__ = "Piotr Gawlowicz"
__copyright__ = "Copyright (c) 2015, Technische Universitat Berlin"
__version__ = "0.1.0"
__email__ = "gawlowicz@tkn.tu-berlin.de"


class RuleDescriptor(object):
    def __init__(self, ruleManager, agentUuid, ruleId, event, filters=[], match=None, action=None, permanence=Permanance.PERSISTENT, ctrl_cb=None):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))
        self.agentUuid = agentUuid
        self.id = ruleId
        self.ruleManager = ruleManager
        self.event = event
        self.filters = filters
        self.match = match
        self.action = action
        self.permanence = permanence
        self.ctrl_cb = ctrl_cb

    def remove(self):
        return self.ruleManager.remove(self.id, self.agentUuid)


class RuleManager(object):
    def __init__(self, controller):
        self.log = logging.getLogger("{module}.{name}".format(
            module=self.__class__.__module__, name=self.__class__.__name__))

        self.controller = controller
        self.ruleIdGen = 0
        
        self.rules_by_node = {}


    def generate_new_rule_id(self):
        self.ruleIdGen = self.ruleIdGen + 1
        return self.ruleIdGen

    def add(self, event, filters=[], match=None, action=None, permanence=Permanance.PERSISTENT, ctrl_callback=None):
        self.log.debug("Adding new rule to node".format())

        destNode = self.controller._scope
        destNode = self.controller.nodeManager.get_node_by_str(destNode)
        destNodeUuid = destNode.id

        rule = {"event":event, "filters":filters, "match":match, "action":action, "permanence":permanence}

        rule_id = self.controller.blocking(True).mgmt.add_rule(rule)
        descriptor = RuleDescriptor(self, destNodeUuid, rule_id, event, filters, match, action, permanence, ctrl_callback)

        if destNodeUuid in self.rules_by_node:
            self.rules_by_node[destNodeUuid].append(descriptor)
        else:
            self.rules_by_node[destNodeUuid] = [descriptor]

        return descriptor


    def remove(self, ruleId, agentUuid=None):
        self.log.debug("remove rule with id: {}".format(ruleId))
        if agentUuid:
            retVal = self.controller.blocking(True).node(agentUuid).mgmt.delete_rule(ruleId)
        else:
            retVal = self.controller.blocking(True).mgmt.delete_rule(ruleId)
        return retVal       
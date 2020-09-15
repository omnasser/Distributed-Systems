#Final Lab
from pox.core import core
import pox.openflow.libopenflow_01 as of
log = core.getLogger()
class Final (object):
  """
  A Firewall object is created for each switch that connects.
  A Connection object for that switch is passed to the __init__ function.
  """
  def __init__ (self, connection):
    # Keep track of the connection to the switch so that we can
    # send it messages!
    self.connection = connection
    # This binds our PacketIn event listener
    connection.addListeners(self)
#-----------------My Code--------------------------------------#

  def do_final (self, packet, packet_in, port_on_switch, switch_id):
    print "work"
    msg = of.ofp_flow_mod()
    msg.match = of.ofp_match.from_packet(packet)
    #msg.buffer_id = packet_in.buffer_id 
    msg.idle_timeout = 60
    msg.hard_timeout = 120

    IP = packet.find('ipv4')
    ICMPtraffic = packet.find('icmp')

    if (IP != None):
      if(ICMPtraffic == None):
       if switch_id !=4:
             if port_on_switch==4:
               msg.actions.append(of.ofp_action_output(port = 1)) 
               msg.data = packet_in
               self.connection.send(msg)
             else:
               msg.actions.append(of.ofp_action_output(port = 4)) 
               msg.data = packet_in
               self.connection.send(msg)

       elif switch_id == 4:
             if IP.dstip == "10.1.1.10":
               msg.actions.append(of.ofp_action_output(port = 5)) 
               msg.data = packet_in
               self.connection.send(msg)

             elif IP.dstip == "10.2.2.20":
               msg.actions.append(of.ofp_action_output(port = 6)) 
               msg.data = packet_in
               self.connection.send(msg)

             elif IP.dstip == "123.45.67.89":
               msg.actions.append(of.ofp_action_output(port = 9)) 
               msg.data = packet_in
               self.connection.send(msg)

             elif IP.dstip == "10.3.3.30":
               msg.actions.append(of.ofp_action_output(port = 7)) 
               msg.data = packet_in
               self.connection.send(msg)

             elif IP.dstip == "10.5.5.50":
                if(IP.srcip == "123.45.67.89"):
                  msg.data = packet_in
                  self.connection.send(msg)
                else:
                  msg.actions.append(of.ofp_action_output(port = 8)) 
                  msg.data = packet_in
                  self.connection.send(msg)
      else:
        if(IP.srcip =="123.45.67.89"):
          msg.data = packet_in
          self.connection.send(msg)
        else:
          msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD)) 
          msg.data = packet_in
          self.connection.send(msg)


                              


    else: #---------Flood all non-IP traffic
      msg.actions.append(of.ofp_action_output(port = of.OFPP_FLOOD))
      msg.data = packet_in
      self.connection.send(msg)


  def _handle_PacketIn (self, event):
    """
    Handles packet in messages from the switch.
    """
    packet = event.parsed # This is the parsed packet data.
    if not packet.parsed:
      log.warning("Ignoring incomplete packet")
      return
    print "debug"
    packet_in = event.ofp # The actual ofp_packet_in message.
    self.do_final(packet, packet_in, event.port, event.dpid)

def launch ():
  print "debug launch"
  """
  Starts the component
  """
  def start_switch (event):
    print "debug start switch"
    log.debug("Controlling %s" % (event.connection,))
    Final(event.connection)
  core.openflow.addListenerByName("ConnectionUp", start_switch)


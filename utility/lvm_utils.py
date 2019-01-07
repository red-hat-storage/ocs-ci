def pvcreate(osd, devices):
    osd.exec_command(cmd='sudo pvcreate %s' % devices)


def vgcreate(osd, vg_name, devices):
    osd.exec_command(cmd='sudo vgcreate %s %s' % (vg_name, devices))
    return vg_name


def lvcreate(osd, lv_name, vg_name, size):
    osd.exec_command(cmd="sudo lvcreate -n %s -l %s %s " % (lv_name, size, vg_name))
    return lv_name


def make_partition(osd, device, start=None, end=None, gpt=False):
    osd.exec_command(cmd='sudo parted --script %s mklabel gpt' % device) if gpt \
        else osd.exec_command(cmd='sudo parted --script %s mkpart primary %s %s' % (device, start, end))


def osd_scenario1(osd, devices_dict, dmcrypt=False):
    """

    :param osd: osd node
    :param devices_dict: dict of devices of the osd node supplied
    :param dmcrypt: False by default
    :return: generated scenario, dmcrypt
    """

    pvcreate(osd, devices_dict.get('devices'))
    vgname = vgcreate(osd, osd.LvmConfig.vg_name % '1', devices_dict.get('device1'))  # dev is a single device
    data_lv1 = lvcreate(osd,
                        osd.LvmConfig.data_lv %
                        '1',
                        osd.LvmConfig.vg_name %
                        '1',
                        osd.LvmConfig.size.format(20))
    data_lv2 = lvcreate(osd,
                        osd.LvmConfig.data_lv %
                        '2',
                        osd.LvmConfig.vg_name %
                        '1',
                        osd.LvmConfig.size.format(20))
    data_lv3 = lvcreate(osd,
                        osd.LvmConfig.data_lv %
                        '3',
                        osd.LvmConfig.vg_name %
                        '1',
                        osd.LvmConfig.size.format(20))
    data_lv4 = lvcreate(osd,
                        osd.LvmConfig.data_lv %
                        '4',
                        osd.LvmConfig.vg_name %
                        '1',
                        osd.LvmConfig.size.format(20))
    db_lv1 = lvcreate(
        osd,
        osd.LvmConfig.db_lv %
        '1',
        osd.LvmConfig.vg_name %
        '1',
        osd.LvmConfig.size.format(8))
    db_lv2 = lvcreate(
        osd,
        osd.LvmConfig.db_lv %
        '2',
        osd.LvmConfig.vg_name %
        '1',
        osd.LvmConfig.size.format(8))
    wal_lv1 = lvcreate(osd,
                       osd.LvmConfig.wal_lv %
                       '1',
                       osd.LvmConfig.vg_name %
                       '1',
                       osd.LvmConfig.size.format(2))
    wal_lv2 = lvcreate(osd,
                       osd.LvmConfig.wal_lv %
                       '2',
                       osd.LvmConfig.vg_name %
                       '1',
                       osd.LvmConfig.size.format(2))

    scenario = "{{'data':'{datalv1}','data_vg':'{vg_name}'}},{{'data':'{datalv2}','data_vg':'{vg_name}'," \
               "'db':'{dblv1}','db_vg':'{vg_name}'}}," \
               "{{'data':'{datalv3}','data_vg':'{vg_name}','wal':'{wallv1}','wal_vg':'{vg_name}'}}," \
               "{{'data':'{datalv4}','data_vg':'{vg_name}','db':'{dblv2}','db_vg':'{vg_name}','wal':'{wallv2}'," \
               "'wal_vg':'{vg_name}'}}".format(vg_name=vgname, datalv1=data_lv1, datalv2=data_lv2, dblv1=db_lv1,
                                               datalv3=data_lv3, wallv1=wal_lv1, datalv4=data_lv4,
                                               dblv2=db_lv2, wallv2=wal_lv2)

    return scenario, dmcrypt


def osd_scenario1_dmcrypt(osd, devices_dict):
    """

    :param osd: osd node
    :param devices_dict: dict of devices of the osd node supplied
    :return: generated scenario, dmcrypt
    """
    scenario, _ = osd_scenario1(osd, devices_dict)
    dmcrypt = True
    return scenario, dmcrypt


def osd_scenario2(osd, devices_dict, dmcrypt=False):
    """

    :param osd:  osd node
    :param devices_dict: dict of devices of the osd node supplied
    :param dmcrypt:  False by default
    :return: generated scenario, dmcrypt
    """
    make_partition(osd, devices_dict.get('device1'), gpt=True)
    make_partition(osd, devices_dict.get('device1'), '1', '80%')
    make_partition(osd, devices_dict.get('device1'), '80%', '85%')
    make_partition(osd, devices_dict.get('device1'), '85%', '90%')
    make_partition(osd, devices_dict.get('device1'), '90%', '95%')
    make_partition(osd, devices_dict.get('device1'), '95%', '100%')

    scenario = "{{'data':'{vdb1}','db':'{vdb2}','wal':'{vdb3}'}},{{'data':'{vdc}','db':'{vdb4}','wal':'{vdb5}'}}," \
               "{{'data':'{vdd}'}}".format(vdb1=devices_dict.get('device1') + '1',
                                           vdb2=devices_dict.get('device1') + '2',
                                           vdb3=devices_dict.get('device1') + '3', vdc=devices_dict.get('device2'),
                                           vdb4=devices_dict.get('device1') + '4',
                                           vdb5=devices_dict.get('device1') + '5', vdd=devices_dict.get('device3'))

    return scenario, dmcrypt


def osd_scenario2_dmcrypt(osd, devices_dict):
    """
    :param osd: osd node
    :param devices_dict: dict of devices of the osd node supplied
    :return: generated scenario, dmcrypt
    """
    scenario, _ = osd_scenario2(osd, devices_dict)
    dmcrypt = True
    return scenario, dmcrypt


def osd_scenario3(osd, devices_dict, dmcrypt=False):
    """
    :param osd:  osd node
    :param devices_dict: dict of devices of the osd node supplied
    :param dmcrypt: False by default
    :return: generated scenario, dmcrypt
    """
    pvcreate(osd, devices_dict.get('devices'))
    vgname = vgcreate(osd, osd.LvmConfig.vg_name % '1', devices_dict.get('device2'))
    make_partition(osd, devices_dict.get('device3'), gpt=True)
    make_partition(osd, devices_dict.get('device3'), '1', '80%')
    make_partition(osd, devices_dict.get('device3'), '80%', '90%')
    make_partition(osd, devices_dict.get('device3'), '90%', '100%')
    data_lv1 = lvcreate(osd,
                        osd.LvmConfig.data_lv %
                        '1',
                        osd.LvmConfig.vg_name %
                        '1',
                        osd.LvmConfig.size.format(80))
    db_lv1 = lvcreate(
        osd,
        osd.LvmConfig.db_lv %
        '1',
        osd.LvmConfig.vg_name %
        '1',
        osd.LvmConfig.size.format(10))
    wal_lv1 = lvcreate(osd,
                       osd.LvmConfig.wal_lv %
                       '1',
                       osd.LvmConfig.vg_name %
                       '1',
                       osd.LvmConfig.size.format(10))

    scenario = "{{'data':'{vdb}','db':'{dblv1}','db_vg':'{vgname}','wal':'{wallv1}','wal_vg':'{vgname}'}}," \
               "{{'data':'{datalv1}','data_vg':'{vgname}','db':'{vdd2}','wal':'{vdd3}'}},{{'data':'{vdd1}'}}" \
        .format(vdb=devices_dict.get('device1'), dblv1=db_lv1, vgname=vgname, wallv1=wal_lv1, datalv1=data_lv1,
                vdd1=devices_dict.get('device3') + '1', vdd2=devices_dict.get('device3') + '2',
                vdd3=devices_dict.get('device3') + '3')

    return scenario, dmcrypt


def osd_scenario3_dmcrypt(osd, devices_dict):
    """
    :param osd: osd node
    :param devices_dict: dict of devices of the osd node supplied
    :return: generated scenario, dmcrypt
    """
    scenario, _ = osd_scenario3(osd, devices_dict)
    dmcrypt = True
    return scenario, dmcrypt

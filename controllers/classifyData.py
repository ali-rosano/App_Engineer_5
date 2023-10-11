async def classify_data(data):
    passport = data.get('passport')
    fullname = data.get('fullname')
    address = data.get('address')
    name = data.get('name')
    lastname = data.get('last_name')
    sex = data.get('sex')

    if passport:
        if name:
            data['sex'] = sex[0] if sex else 'Not specified'
            data['fullname'] = f"{name} {lastname}"
            del data['name']
            del data['last_name']
        return 'passport', passport, data
    elif address:
        return 'address', address, data
    elif fullname:
        return 'fullname', fullname, data
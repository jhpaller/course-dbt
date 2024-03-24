SELECT ID,
       NAME,
       GENDER,
       EYE_COLOR,
       RACE,
       HAIR_COLOR,
       HEIGHT,
       PUBLISHER,
       SKIN_COLOR,
       ALIGNMENT,
       WEIGHT,
       CREATED_AT,
       UPDATED_AT
FROM {{ source('postgres', 'superheroes')}}